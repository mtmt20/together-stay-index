"""
Together-Stay Index - 맛집/카페 크롤러 (숙소 크롤러의 확장)
=======================================================

숙소 크롤링만으로는 "근처 갈만한 곳"이라는 원래 기획의 절반만 채워진다.
이 모듈은 숙소 크롤러(naver_crawler.py)가 이미 검증한 방식(네이버 검색
결과 페이지 직접 스크래핑, BeautifulSoup 본문 보강)을 그대로 재사용해서
지역별 "아이랑 갈만한 맛집"과 "노키즈존 아닌 카페"를 수집한다.

왜 새 공공데이터 API를 안 쓰고 크롤링 방식을 그대로 확장했나
------------------------------------------------------------
"재방문율", "노키즈존 여부" 같은 건 어떤 공공데이터에도 없는 값이다.
이런 건 결국 사람들이 후기에 남긴 말("재방문했어요", "노키즈존이라
아쉬웠어요")에서 뽑아낼 수밖에 없어서, 이미 갖고 있는 소셜 크롤링
파이프라인을 그대로 확장하는 게 정확도와 개발 속도 둘 다에서 낫다.
(대형마트/응급실처럼 정말 "공공데이터 조회"가 맞는 값은 별도 API 연동이
더 적합하지만, 그건 신청/승인 대기 시간이 있어서 이번 범위에서는 뺐다.)

지역 목록에 대한 한계
--------------------
REGIONS는 지금까지 숙소 크롤링 결과에 실제로 등장한 지역명을 손으로
추린 것이다(하드코딩). 나중에 Silver 레이어(stg_naver_posts)에서 지역을
자동으로 뽑아 이 목록을 매번 갱신하도록 개선할 수 있지만, 이번 범위에서는
정적 목록으로 충분하다고 판단했다.
"""

from __future__ import annotations

import io
import logging
import os
import re
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Iterable

import boto3
import pandas as pd

from crawler.naver_crawler import (
    CRAWL_DELAY_SECONDS,
    NAVER_SEARCH_PAGES,
    RESULTS_PER_KEYWORD,
    fetch_full_text,
    search_naver,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# 숙소 크롤링 결과에 실제로 나온 지역들 (2026-09-18 기준 관찰).
REGIONS: tuple[str, ...] = (
    "제주", "문경", "가평", "포천", "안동", "경주", "고창", "청도",
    "강화도", "대부도", "밀양", "태안", "양평", "제천", "영광",
)

# 카테고리별 검색 키워드 템플릿과, 결과에 태깅할 신호 단어들.
CATEGORY_CONFIG: dict[str, dict] = {
    "food": {
        "keyword_template": "{region} 아이랑 맛집",
        "exclude_terms": (),
        "exclude_exceptions": (),
        "tag_terms": {
            "mentions_revisit": ("재방문", "또 방문", "또 갔"),
            "mentions_kids_friendly": ("키즈메뉴", "유아의자", "놀이방", "아이랑", "아기의자"),
        },
    },
    "cafe": {
        "keyword_template": "{region} 노키즈존 아닌 카페",
        # "노키즈존"이 언급됐어도 "아님/아니/해제/가능"과 같이 나오면
        # (즉 "노키즈존 아니에요" 같은 긍정 문맥이면) 제외하지 않는다.
        "exclude_terms": ("노키즈존",),
        "exclude_exceptions": ("아님", "아니", "해제", "가능", "허용"),
        "tag_terms": {
            "mentions_nature": ("자연", "마당", "정원", "숲", "뷰"),
            "mentions_kids_friendly": ("아이랑", "유아의자", "키즈"),
        },
    },
}

# 본문에서 인원/가격을 추정하는 정규식. 정확한 구조화 데이터가 아니라
# "언급된 첫 번째 그럴듯한 값" 수준의 힌트로만 쓴다.
_CAPACITY_PATTERN = re.compile(r"(\d{1,2})\s*(?:인|명)")
_PRICE_PATTERN = re.compile(r"(\d{1,3}(?:,\d{3})+|\d{4,6})\s*원")

S3_KEY_PREFIX = "bronze/together-stay-index/food_cafe_raw"


@dataclass
class FoodCafePost:
    category: str  # "food" 또는 "cafe"
    region: str
    keyword: str
    source: str  # "blog" 또는 "cafearticle"
    title: str
    link: str
    description: str
    full_text: str | None
    crawled_at: str
    mentions_revisit: bool
    mentions_kids_friendly: bool
    mentions_nature: bool
    capacity_hint: int | None
    price_hint: int | None


def _extract_capacity(text: str) -> int | None:
    match = _CAPACITY_PATTERN.search(text)
    return int(match.group(1)) if match else None


def _extract_price(text: str) -> int | None:
    match = _PRICE_PATTERN.search(text)
    if not match:
        return None
    return int(match.group(1).replace(",", ""))


def _should_exclude(text: str, exclude_terms: tuple[str, ...], exceptions: tuple[str, ...]) -> bool:
    for term in exclude_terms:
        idx = text.find(term)
        if idx == -1:
            continue
        # 제외 단어 주변(±10자)에 예외 단어가 있으면 실제로는 긍정 문맥이므로 제외하지 않는다.
        window = text[max(0, idx - 10): idx + len(term) + 10]
        if not any(exc in window for exc in exceptions):
            return True
    return False


def crawl_category_for_region(category: str, region: str, with_full_text: bool = True) -> list[FoodCafePost]:
    """카테고리(food/cafe) 하나 x 지역 하나에 대해 blog+cafearticle 검색."""
    config = CATEGORY_CONFIG[category]
    keyword = config["keyword_template"].format(region=region)
    posts: list[FoodCafePost] = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for source in NAVER_SEARCH_PAGES:
        items = search_naver(keyword, source)
        time.sleep(CRAWL_DELAY_SECONDS)

        for item in items[:RESULTS_PER_KEYWORD]:
            title = item.get("title", "")
            description = item.get("description", "")
            combined = title + description

            if _should_exclude(combined, config["exclude_terms"], config["exclude_exceptions"]):
                continue

            full_text = None
            if with_full_text:
                full_text = fetch_full_text(item["link"])
                time.sleep(CRAWL_DELAY_SECONDS)

            full_combined = combined + (full_text or "")
            tag_terms = config["tag_terms"]
            posts.append(
                FoodCafePost(
                    category=category,
                    region=region,
                    keyword=keyword,
                    source=source,
                    title=title,
                    link=item.get("link", ""),
                    description=description,
                    full_text=full_text,
                    crawled_at=now_iso,
                    mentions_revisit=any(t in full_combined for t in tag_terms.get("mentions_revisit", ())),
                    mentions_kids_friendly=any(t in full_combined for t in tag_terms.get("mentions_kids_friendly", ())),
                    mentions_nature=any(t in full_combined for t in tag_terms.get("mentions_nature", ())),
                    capacity_hint=_extract_capacity(full_combined),
                    price_hint=_extract_price(full_combined),
                )
            )

    return posts


def crawl_all_regions(
    regions: Iterable[str] = REGIONS,
    categories: Iterable[str] = ("food", "cafe"),
    with_full_text: bool = True,
) -> pd.DataFrame:
    """모든 지역 x 카테고리 조합을 순회하며 크롤링한다."""
    all_posts: list[FoodCafePost] = []
    for region in regions:
        for category in categories:
            all_posts.extend(crawl_category_for_region(category, region, with_full_text=with_full_text))

    df = pd.DataFrame([asdict(p) for p in all_posts])
    logger.info("맛집/카페 전체 수집 건수: %d", len(df))
    return df


def upload_dataframe_to_s3(df: pd.DataFrame, bucket: str | None = None) -> str:
    bucket = bucket or os.environ["AWS_S3_BUCKET"]

    now = datetime.now(timezone.utc)
    date_partition = now.strftime("%Y-%m-%d")
    timestamp = now.strftime("%Y%m%dT%H%M%S")
    s3_key = f"{S3_KEY_PREFIX}/dt={date_partition}/food_cafe_raw_{timestamp}.parquet"

    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    buffer.seek(0)

    s3_client = boto3.client("s3")
    s3_client.upload_fileobj(buffer, bucket, s3_key)
    logger.info("S3 업로드 완료: s3://%s/%s (%d rows)", bucket, s3_key, len(df))

    return s3_key


def run(with_full_text: bool = True) -> str:
    df = crawl_all_regions(with_full_text=with_full_text)
    if df.empty:
        logger.warning("수집된 데이터가 없어 업로드를 건너뜁니다.")
        return ""
    return upload_dataframe_to_s3(df)


if __name__ == "__main__":
    run()

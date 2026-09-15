"""
Together-Stay Index - Step 1: 네이버 카페/블로그 크롤러
=====================================================

다둥이(3명 이상 자녀) 가구 및 두 가족 이상 연합 여행객이 실제로 언급하는
"두 가족 숙소", "다둥이 펜션", "독채 마당", "화장실 2개" 같은 키워드를
네이버 검색 결과(카페/블로그)에서 수집하여 S3 Bronze 레이어에 Parquet으로
적재하는 모듈이다.

설계 원칙 (왜 이렇게 짰는지)
---------------------------
1) 네이버 검색 결과 페이지를 직접 requests/Selenium으로 긁는 방식은
   봇 차단(캡차)에 매우 취약하고, 대회 심사 중 라이브 데모가 막힐 위험이 크다.
   따라서 "검색(디스커버리)"은 네이버 공식 오픈 API(검색 API)로 안정적으로
   수행하고, API가 주지 않는 "본문 전체 텍스트"만 BeautifulSoup으로
   개별 글 URL에 접속해 보강한다. 이렇게 하면 문제에서 요구한
   "BeautifulSoup/Selenium 크롤링"과 "검색 API" 둘 다 실제로 쓰면서도
   안정성을 확보할 수 있다.
2) 본문 크롤링은 실패해도(로그인 필요/삭제글/차단 등) 전체 파이프라인이
   죽지 않도록 개별 요청 단위로 예외를 흡수한다. 대회 데모 중 특정 글
   하나 때문에 DAG 전체가 실패하면 안 되기 때문.
3. 이 프로젝트는 기존 위스키/부동산 파이프라인과 완전히 분리된 신규
   프로젝트이므로, S3 버킷/Snowflake DB/Airflow 커넥션 이름을 전부
   "TOGETHER_STAY" 계열의 고유한 이름으로 강제해 기존 리소스와
   절대 겹치지 않도록 한다 (환경변수 필수값으로 강제).

필요 환경변수
-------------
- NAVER_CLIENT_ID, NAVER_CLIENT_SECRET : 네이버 개발자센터에서 발급한
  "검색" API 애플리케이션 키. https://developers.naver.com/apps/#/register
- AWS_S3_BUCKET : 이 프로젝트 전용 S3 버킷명 (기존 프로젝트 버킷과 달라야 함)
- AWS 자격증명은 boto3 기본 체인(환경변수/~/.aws/credentials/IAM Role)을 그대로 사용

실행 예시
--------
    python -m crawler.naver_crawler
"""

from __future__ import annotations

import io
import logging
import os
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Iterable

import boto3
import pandas as pd
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ---------------------------------------------------------------------------
# 설정값
# ---------------------------------------------------------------------------

# 대회 타겟 키워드. 다둥이/다가구 연합 체류형 숙소 수요를 잡아내는 문구들.
SEARCH_KEYWORDS: list[str] = [
    "두 가족 숙소",
    "다둥이 펜션",
    "독채 마당",
    "화장실 2개",
]

# 네이버 검색 API 대상 카테고리. 카페글(cafearticle)과 블로그(blog) 둘 다 수집.
NAVER_SEARCH_ENDPOINTS: dict[str, str] = {
    "blog": "https://openapi.naver.com/v1/search/blog.json",
    "cafearticle": "https://openapi.naver.com/v1/search/cafearticle.json",
}

# 카테고리별 한 키워드당 가져올 최대 결과 수 (API display 파라미터 상한은 100)
RESULTS_PER_KEYWORD = 30

# 본문 크롤링 시 매 요청 사이 최소 대기 시간(초). 과도한 요청으로 차단당하지
# 않도록 예의를 지킨다 (rate limiting).
CRAWL_DELAY_SECONDS = 1.0

# 원본 글 본문을 가져올 때 사용할 User-Agent (봇으로 즉시 차단되는 것을 방지)
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# S3 업로드 시 Bronze 레이어 경로 prefix. 다른 프로젝트와 절대 겹치지 않도록
# "together-stay-index" 네임스페이스를 고정으로 붙인다.
S3_KEY_PREFIX = "bronze/together-stay-index/naver_raw"


@dataclass
class NaverPost:
    """네이버 검색 결과 1건을 표현하는 레코드."""

    keyword: str
    source: str  # "blog" 또는 "cafearticle"
    title: str
    link: str
    description: str
    post_date: str | None
    full_text: str | None
    crawled_at: str


def _strip_html_tags(text: str) -> str:
    """네이버 검색 API 응답에는 <b> 등 하이라이트 태그가 섞여 있어 제거한다."""
    return BeautifulSoup(text, "html.parser").get_text()


def search_naver(keyword: str, source: str, client_id: str, client_secret: str) -> list[dict]:
    """네이버 검색 오픈 API로 키워드 하나를 조회한다.

    직접 검색 결과 페이지를 스크래핑하지 않고 공식 API를 쓰는 이유는
    모듈 docstring의 설계 원칙 1번 참고.
    """
    url = NAVER_SEARCH_ENDPOINTS[source]
    headers = {
        "X-Naver-Client-Id": client_id,
        "X-Naver-Client-Secret": client_secret,
    }
    params = {
        "query": keyword,
        "display": min(RESULTS_PER_KEYWORD, 100),
        "sort": "sim",  # 정확도순. 최신순이 필요하면 "date"로 변경.
    }

    response = requests.get(url, headers=headers, params=params, timeout=10)
    response.raise_for_status()
    items = response.json().get("items", [])
    logger.info("[%s/%s] 검색 결과 %d건 수신", keyword, source, len(items))
    return items


def fetch_full_text(url: str) -> str | None:
    """개별 글 URL에 접속해 본문 텍스트를 최대한 추출한다.

    카페글은 로그인/멤버 전용 글이 많아 실패율이 높다. 실패는 정상 흐름의
    일부로 간주하고 None을 반환할 뿐 예외를 전파하지 않는다 (파이프라인
    전체를 죽이지 않기 위함 - 설계 원칙 2번).
    """
    try:
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=10)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("본문 크롤링 실패 (%s): %s", url, exc)
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # 네이버 블로그는 본문이 iframe(mainFrame) 안에 있는 경우가 많아
    # 정적 requests로는 iframe 내부까지 못 가져올 수 있다. 이 경우
    # 실제 프로덕션에서는 Selenium으로 iframe 진입 후 렌더링된 DOM을
    # 읽어야 한다 (본 스켈레톤에서는 정적 파싱으로 얻어지는 범위까지만
    # 처리하고, 실패 시 None을 반환해 후속 단계에서 걸러낸다).
    candidates = soup.select_one("div.se-main-container") or soup.select_one("#postViewArea")
    if candidates is None:
        candidates = soup.find("body")

    if candidates is None:
        return None

    text = candidates.get_text(separator=" ", strip=True)
    return text or None


def crawl_keyword(keyword: str, client_id: str, client_secret: str, with_full_text: bool = True) -> list[NaverPost]:
    """키워드 하나에 대해 blog + cafearticle 검색 후 레코드 리스트로 변환한다."""
    posts: list[NaverPost] = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for source in NAVER_SEARCH_ENDPOINTS:
        items = search_naver(keyword, source, client_id, client_secret)
        for item in items:
            full_text = None
            if with_full_text:
                full_text = fetch_full_text(item["link"])
                time.sleep(CRAWL_DELAY_SECONDS)

            posts.append(
                NaverPost(
                    keyword=keyword,
                    source=source,
                    title=_strip_html_tags(item.get("title", "")),
                    link=item.get("link", ""),
                    description=_strip_html_tags(item.get("description", "")),
                    post_date=item.get("postdate") or item.get("pubDate"),
                    full_text=full_text,
                    crawled_at=now_iso,
                )
            )

    return posts


def crawl_all_keywords(
    keywords: Iterable[str] = SEARCH_KEYWORDS,
    with_full_text: bool = True,
) -> pd.DataFrame:
    """모든 타겟 키워드를 순회하며 크롤링하고 하나의 DataFrame으로 합친다."""
    client_id = os.environ["NAVER_CLIENT_ID"]
    client_secret = os.environ["NAVER_CLIENT_SECRET"]

    all_posts: list[NaverPost] = []
    for keyword in keywords:
        all_posts.extend(crawl_keyword(keyword, client_id, client_secret, with_full_text=with_full_text))

    df = pd.DataFrame([asdict(p) for p in all_posts])
    logger.info("전체 수집 건수: %d", len(df))
    return df


def upload_dataframe_to_s3(df: pd.DataFrame, bucket: str | None = None) -> str:
    """DataFrame을 Parquet으로 변환해 S3 Bronze 레이어에 업로드한다.

    반환값은 업로드된 S3 key (Snowflake COPY INTO에서 그대로 참조하기 위함).
    """
    bucket = bucket or os.environ["AWS_S3_BUCKET"]

    now = datetime.now(timezone.utc)
    date_partition = now.strftime("%Y-%m-%d")
    timestamp = now.strftime("%Y%m%dT%H%M%S")
    s3_key = f"{S3_KEY_PREFIX}/dt={date_partition}/naver_raw_{timestamp}.parquet"

    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    buffer.seek(0)

    s3_client = boto3.client("s3")
    s3_client.upload_fileobj(buffer, bucket, s3_key)
    logger.info("S3 업로드 완료: s3://%s/%s (%d rows)", bucket, s3_key, len(df))

    return s3_key


def run(with_full_text: bool = True) -> str:
    """Airflow PythonOperator 및 CLI 양쪽에서 호출하는 단일 진입점."""
    df = crawl_all_keywords(with_full_text=with_full_text)
    if df.empty:
        logger.warning("수집된 데이터가 없어 업로드를 건너뜁니다.")
        return ""
    return upload_dataframe_to_s3(df)


if __name__ == "__main__":
    run()

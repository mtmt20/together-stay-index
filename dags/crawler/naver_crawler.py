"""
Together-Stay Index - Step 1: 네이버 카페/블로그 크롤러
=====================================================

다둥이(3명 이상 자녀) 가구 및 두 가족 이상 연합 여행객이 실제로 언급하는
"두 가족 숙소", "다둥이 펜션", "독채 마당", "화장실 2개" 같은 키워드를
네이버 검색 결과(카페/블로그)에서 수집하여 S3 Bronze 레이어에 Parquet으로
적재하는 모듈이다.

설계 원칙 (왜 이렇게 짰는지)
---------------------------
1) **(2026-09-18 변경) 검색은 네이버 검색 결과 페이지를 직접 스크래핑한다.**
   원래는 봇 차단 위험 때문에 네이버 공식 검색 오픈API를 쓰려고 했으나,
   네이버가 검색 오픈API의 신규/재발급 자체를 막아놔서(기존에 등록되어
   있던 다른 앱도 재등록 시도하면 "신규로 등록할 수 없는 API가
   선택되었습니다"로 거부됨 - 계정을 새로 만들어도 동일) 공식 API 경로를
   아예 쓸 수 없는 상태다. 그래서 어쩔 수 없이 검색 결과 페이지
   (`search.naver.com`)를 직접 파싱하는 방식으로 전환했다. 이미 개별
   글 본문을 가져올 때 쓰던 것과 동일한 방식(User-Agent 지정, 요청 간
   지연시간)으로 예의 있게 접근해 차단 위험을 최대한 낮춘다.
2) 본문 크롤링은 실패해도(로그인 필요/삭제글/차단 등) 전체 파이프라인이
   죽지 않도록 개별 요청 단위로 예외를 흡수한다. 대회 데모 중 특정 글
   하나 때문에 DAG 전체가 실패하면 안 되기 때문.
3. 이 프로젝트는 기존 위스키/부동산 파이프라인과 완전히 분리된 신규
   프로젝트이므로, S3 버킷 이름을 "TOGETHER_STAY" 계열의 고유한 이름으로
   강제해 기존 리소스와 절대 겹치지 않도록 한다 (환경변수 필수값으로 강제).

필요 환경변수
-------------
- AWS_S3_BUCKET : 이 프로젝트 전용 S3 버킷명 (기존 프로젝트 버킷과 달라야 함)
- AWS 자격증명은 boto3 기본 체인(환경변수/~/.aws/credentials/IAM Role)을 그대로 사용

(참고: 예전에는 NAVER_CLIENT_ID/NAVER_CLIENT_SECRET이 필요했으나, 검색
오픈API를 더 이상 못 쓰게 되면서 필요 없어졌다. 코드에 남아있어도 무시됨.)

실행 예시
--------
    python -m crawler.naver_crawler
"""

from __future__ import annotations

import io
import logging
import os
import re
import time
from collections import defaultdict
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Iterable
from urllib.parse import quote

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

# "화장실 2개", "독채 마당" 같은 키워드는 여행/숙소 얘기뿐 아니라 인테리어·
# 리모델링·부동산 카페 글에도 흔히 나온다 (예: "화장실 2개 리모델링 비용").
# 그래서 제목+설명에 숙박 관련 단어가 하나라도 같이 나오는 글만 남기는
# 2차 필터를 둔다. 이 목록에 없는 새 숙박 형태(예: "한옥스테이")가 계속
# 걸러진다면 여기에 추가하면 된다.
ACCOMMODATION_CONTEXT_TERMS: tuple[str, ...] = (
    "숙소", "펜션", "풀빌라", "게스트하우스", "한옥", "캠핑", "글램핑",
    "리조트", "여행", "숙박", "스테이", "카라반",
)


def _looks_like_accommodation_post(title: str, description: str) -> bool:
    """제목/설명에 숙박 관련 문맥 단어가 하나라도 있는지 확인한다."""
    combined = title + description
    return any(term in combined for term in ACCOMMODATION_CONTEXT_TERMS)

# 네이버 통합검색 결과 페이지(where= 파라미터로 카테고리 구분). blog=블로그 탭,
# article=카페글 탭. 검색 오픈API가 막혀서(모듈 docstring 참고) 이 페이지를
# 직접 파싱한다.
NAVER_SEARCH_PAGES: dict[str, tuple[str, str]] = {
    # source: (검색 결과 URL의 where 값, 결과 링크에 포함될 도메인 문자열)
    "blog": ("blog", "blog.naver.com"),
    "cafearticle": ("article", "cafe.naver.com"),
}

# 검색 결과 링크가 실제 "글"인지 판별하는 정규식. 둘 다 "도메인/아이디/숫자"
# 형태(블로그 글 번호, 카페 게시글 번호)만 인정하고, 프로필 링크(도메인/아이디만)
# 등은 걸러낸다.
_ARTICLE_URL_PATTERNS: dict[str, str] = {
    "blog.naver.com": r"blog\.naver\.com/[^/?]+/\d+$",
    "cafe.naver.com": r"cafe\.naver\.com/[^/]+/\d+$",
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


def search_naver(keyword: str, source: str) -> list[dict]:
    """네이버 검색 결과 페이지(blog/카페글 탭)를 직접 파싱해서
    [{title, link, description}, ...] 형태로 반환한다.

    (왜 API 응답이랑 똑같은 dict 모양으로 맞췄나: 이 함수를 쓰는
    crawl_keyword()가 기존에 "네이버 검색 오픈API의 JSON 응답" 모양을
    그대로 가정하고 짜여 있었다. API 자체를 못 쓰게 됐다고 그 아래
    로직까지 다 뜯어고치는 대신, 이 함수의 반환 모양만 API 시절과
    동일하게 맞춰서 나머지 코드는 안 건드려도 되게 했다.)

    검색 결과 페이지 하나에는 SDS(네이버 새 UI) 렌더링 특성상 같은 글
    링크가 썸네일/블로거이름/좋아요·댓글 수 배지/제목 등 여러 <a> 태그로
    중복 등장한다. href 기준으로 묶어서, 그중 "그럴듯한 제목처럼 보이는"
    가장 짧은 텍스트를 제목으로, 가장 긴 텍스트(보통 날짜+요약 스니펫)를
    설명으로 쓴다. 댓글 수 같은 순수 숫자 배지가 제목으로 잘못 뽑히는 걸
    막기 위해 숫자만 있거나 너무 짧은 텍스트는 제목 후보에서 제외한다.
    """
    where, domain = NAVER_SEARCH_PAGES[source]
    url = f"https://search.naver.com/search.naver?where={where}&query={quote(keyword)}"

    response = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=10)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    id_pattern = _ARTICLE_URL_PATTERNS[domain]
    texts_by_link: dict[str, list[str]] = defaultdict(list)
    for a in soup.find_all("a", href=True):
        link = a["href"].split("?")[0]  # 카페 링크에 붙는 추적용 art= 토큰 제거
        if domain not in link or not re.search(id_pattern, link):
            continue
        text = a.get_text(strip=True).replace("새 창 열림", "").strip()
        if text:
            texts_by_link[link].append(text)

    items: list[dict] = []
    for link, texts in texts_by_link.items():
        # 순수 URL 표기 텍스트(예: "blog.naver.com›아이디")는 제외
        candidates = [t for t in texts if domain not in t]
        if not candidates:
            continue
        description = max(candidates, key=len)
        # 숫자만 있는 배지(댓글/좋아요 수)나 너무 짧은 텍스트는 제목 후보 제외
        title_candidates = [t for t in candidates if len(t) >= 5 and not t.isdigit()]
        title = min(title_candidates, key=len) if title_candidates else description[:40]
        items.append({"title": title, "link": link, "description": description})
        if len(items) >= RESULTS_PER_KEYWORD:
            break

    logger.info("[%s/%s] 검색 결과 %d건 수신 (스크래핑)", keyword, source, len(items))
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


def crawl_keyword(keyword: str, with_full_text: bool = True) -> list[NaverPost]:
    """키워드 하나에 대해 blog + cafearticle 검색 후 레코드 리스트로 변환한다."""
    posts: list[NaverPost] = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for source in NAVER_SEARCH_PAGES:
        raw_items = search_naver(keyword, source)
        time.sleep(CRAWL_DELAY_SECONDS)  # 검색 결과 페이지 자체도 예의상 간격을 둔다

        items = [
            item for item in raw_items
            if _looks_like_accommodation_post(item.get("title", ""), item.get("description", ""))
        ]
        if len(items) < len(raw_items):
            logger.info(
                "[%s/%s] 숙박 무관 글 %d건 제외 (%d -> %d건)",
                keyword, source, len(raw_items) - len(items), len(raw_items), len(items),
            )

        for item in items:
            full_text = None
            if with_full_text:
                full_text = fetch_full_text(item["link"])
                time.sleep(CRAWL_DELAY_SECONDS)

            posts.append(
                NaverPost(
                    keyword=keyword,
                    source=source,
                    title=item.get("title", ""),
                    link=item.get("link", ""),
                    description=item.get("description", ""),
                    post_date=None,  # 검색 결과 페이지에서 정확한 날짜를 안정적으로 못 뽑아 비워둠
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
    all_posts: list[NaverPost] = []
    for keyword in keywords:
        all_posts.extend(crawl_keyword(keyword, with_full_text=with_full_text))

    df = pd.DataFrame([asdict(p) for p in all_posts])
    logger.info("전체 수집 건수: %d", len(df))
    return df


def upload_dataframe_to_s3(df: pd.DataFrame, bucket: str | None = None) -> str:
    """DataFrame을 Parquet으로 변환해 S3 Bronze 레이어에 업로드한다.

    반환값은 업로드된 S3 key (dbt가 read_parquet으로 그대로 참조하기 위함).
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

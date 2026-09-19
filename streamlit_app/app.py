"""
Together-Stay Index - Step 4: Streamlit 대시보드 (PoC)
======================================================

dbt Gold 레이어(gold.mart_keyword_daily_trend, gold.mart_candidate_posts,
gold.mart_food_cafe)만 읽는 얇은 조회 화면이다.

2026-09-18: Snowflake -> DuckDB 이전.
  - 로컬 Docker: tsi-dbt가 만든 .duckdb 파일을 tsi-streamlit도 같은 볼륨
    (./warehouse)으로 마운트해서 그냥 읽기 전용으로 연다. 계정/비밀번호가
    아예 없다.
  - Streamlit Community Cloud: 로컬 파일을 마운트할 수 없으므로, dbt가
    dbt run 직후 S3에 올려둔 스냅샷(gold-warehouse/together_stay.duckdb)을
    앱 시작 시 내려받아 임시 파일로 연다. 이때만 AWS 자격증명이 필요하고
    Snowflake 키페어 같은 복잡한 설정이 없어 secrets 입력도 훨씬 쉬워졌다.
"""

from __future__ import annotations

import os
import tempfile

import duckdb
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Together-Stay Index", layout="wide")

# 쿼리/커넥션 캐시 유효시간(초). 데모 직전에만 짧게 낮추면 된다.
CACHE_TTL_SECONDS = 3600

# 로컬 Docker에서는 이 경로가 실제로 마운트되어 있다. Streamlit Cloud에는
# 이 파일이 없으므로 S3에서 내려받는 경로로 분기한다.
LOCAL_DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "/data/together_stay.duckdb")
S3_WAREHOUSE_KEY = "gold-warehouse/together_stay.duckdb"


def _config(key: str, default: str | None = None) -> str | None:
    """설정값을 두 군데에서 찾는다: 로컬 Docker는 환경변수, Streamlit
    Community Cloud는 st.secrets(대시보드 설정 화면에 입력한 값)를 쓴다."""
    try:
        if key in st.secrets:
            return st.secrets[key]
    except Exception:  # noqa: BLE001 - secrets.toml이 아예 없는 로컬 환경 대비
        pass
    return os.environ.get(key, default)


@st.cache_resource(ttl=CACHE_TTL_SECONDS)
def get_connection() -> duckdb.DuckDBPyConnection:
    """DuckDB 커넥션은 세션당 한 번만 생성해 재사용한다."""
    if os.path.exists(LOCAL_DUCKDB_PATH):
        return duckdb.connect(LOCAL_DUCKDB_PATH, read_only=True)

    import boto3

    s3 = boto3.client(
        "s3",
        aws_access_key_id=_config("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=_config("AWS_SECRET_ACCESS_KEY"),
        region_name=_config("AWS_REGION", "ap-northeast-2"),
    )
    tmp_path = os.path.join(tempfile.gettempdir(), "together_stay_snapshot.duckdb")
    s3.download_file(_config("AWS_S3_BUCKET"), S3_WAREHOUSE_KEY, tmp_path)
    return duckdb.connect(tmp_path, read_only=True)


def _query(sql: str) -> pd.DataFrame:
    df = get_connection().execute(sql).fetchdf()
    df.columns = [c.upper() for c in df.columns]
    return df


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_keyword_trend() -> pd.DataFrame:
    return _query(
        "select keyword, crawled_date, mention_count, unique_post_count "
        "from gold.mart_keyword_daily_trend order by crawled_date"
    )


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_candidate_posts() -> pd.DataFrame:
    return _query(
        "select keyword, source, title, link, crawled_at, "
        "mentions_bathroom, mentions_yard, mentions_whole_house "
        "from gold.mart_candidate_posts order by crawled_at desc"
    )


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_food_cafe() -> pd.DataFrame:
    df = _query(
        "select category, region, title, link, source, crawled_at, "
        "mentions_revisit, mentions_kids_friendly, mentions_nature, "
        "capacity_hint, price_hint, concept_tags, recommend_score "
        "from gold.mart_food_cafe order by region, category, recommend_score desc"
    )
    # concept_tags는 DuckDB LIST 컬럼이라 파이썬 리스트/배열로 그대로 온다 -
    # 화면 표시용으로 문자열로 풀어준다.
    if not df.empty:
        df["CONCEPT_TAGS"] = df["CONCEPT_TAGS"].apply(
            lambda v: ", ".join(list(v)) if v is not None and len(v) > 0 else ""
        )
    return df


st.title("Together-Stay Index")
st.caption("다둥이 / 다가구 연합 체류형 숙소 수요 모니터링 대시보드 (PoC)")

col1, col2 = st.columns([3, 1])
with col2:
    if st.button("데이터 새로고침 (캐시 초기화)"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()

try:
    trend_df = load_keyword_trend()
    posts_df = load_candidate_posts()
    food_cafe_df = load_food_cafe()
except Exception as exc:  # noqa: BLE001 - 대시보드 최상단에서 원인을 그대로 보여주기 위함
    st.error(
        "DuckDB 연결/조회에 실패했습니다. dbt run(및 publish_to_s3.py)이 "
        "먼저 수행되었는지 확인하세요."
    )
    st.exception(exc)
    st.stop()

# --- KPI 요약 ---------------------------------------------------------
k1, k2, k3 = st.columns(3)
k1.metric("누적 언급 글 수", int(trend_df["MENTION_COUNT"].sum()) if not trend_df.empty else 0)
k2.metric("고유 글(URL) 수", int(trend_df["UNIQUE_POST_COUNT"].sum()) if not trend_df.empty else 0)
k3.metric("본문 확보된 후보 글 수", len(posts_df))

# --- 키워드별 일자 트렌드 ------------------------------------------------
st.subheader("키워드별 언급 추이")
if trend_df.empty:
    st.info("아직 적재된 데이터가 없습니다. Airflow DAG와 dbt run을 먼저 실행하세요.")
else:
    pivot = trend_df.pivot(index="CRAWLED_DATE", columns="KEYWORD", values="MENTION_COUNT").fillna(0)
    st.line_chart(pivot)

# --- 후보 글 큐레이션 테이블 ---------------------------------------------
st.subheader("숙소 후보 언급 글")

available_keywords = sorted(posts_df["KEYWORD"].unique().tolist()) if not posts_df.empty else []
selected_keywords = st.multiselect("키워드 필터", available_keywords, default=available_keywords)

only_with_signal = st.checkbox("화장실/마당/독채 언급이 있는 글만 보기", value=False)

filtered = posts_df[posts_df["KEYWORD"].isin(selected_keywords)] if selected_keywords else posts_df
if only_with_signal:
    filtered = filtered[
        filtered["MENTIONS_BATHROOM"] | filtered["MENTIONS_YARD"] | filtered["MENTIONS_WHOLE_HOUSE"]
    ]

st.dataframe(
    filtered[["KEYWORD", "SOURCE", "TITLE", "LINK", "CRAWLED_AT",
              "MENTIONS_BATHROOM", "MENTIONS_YARD", "MENTIONS_WHOLE_HOUSE"]],
    use_container_width=True,
    hide_index=True,
)

# --- 지역별 갈만한 맛집/카페 ---------------------------------------------
st.divider()
st.subheader("지역별 갈만한 맛집 · 카페")
st.caption(
    "맛집은 재방문 언급이 있는 곳을 우선 정렬, 카페는 노키즈존이 아니면서 "
    "자연·아동친화 요소가 있는 곳만 모았습니다."
)

if food_cafe_df.empty:
    st.info("아직 맛집/카페 데이터가 없습니다. food_cafe_crawler DAG 태스크를 먼저 실행하세요.")
else:
    fc_col1, fc_col2 = st.columns(2)
    with fc_col1:
        region_options = sorted(food_cafe_df["REGION"].unique().tolist())
        selected_regions = st.multiselect("지역 필터", region_options, default=region_options)
    with fc_col2:
        category_label = {"food": "맛집", "cafe": "카페"}
        selected_category_labels = st.multiselect(
            "카테고리", list(category_label.values()), default=list(category_label.values())
        )
        selected_categories = [k for k, v in category_label.items() if v in selected_category_labels]

    fc_filtered = food_cafe_df[
        food_cafe_df["REGION"].isin(selected_regions) & food_cafe_df["CATEGORY"].isin(selected_categories)
    ]

    food_tab, cafe_tab = st.tabs(["🍽️ 맛집 (재방문순)", "☕ 카페 (자연·아동친화)"])

    with food_tab:
        food_view = fc_filtered[fc_filtered["CATEGORY"] == "food"].sort_values(
            "RECOMMEND_SCORE", ascending=False
        )
        st.dataframe(
            food_view[["REGION", "TITLE", "LINK", "CONCEPT_TAGS", "CAPACITY_HINT", "PRICE_HINT", "CRAWLED_AT"]]
            .rename(columns={
                "REGION": "지역", "TITLE": "제목", "LINK": "링크", "CONCEPT_TAGS": "컨셉",
                "CAPACITY_HINT": "인원(추정)", "PRICE_HINT": "가격(추정,원)", "CRAWLED_AT": "수집시각",
            }),
            use_container_width=True,
            hide_index=True,
        )

    with cafe_tab:
        cafe_view = fc_filtered[fc_filtered["CATEGORY"] == "cafe"].sort_values(
            "RECOMMEND_SCORE", ascending=False
        )
        st.dataframe(
            cafe_view[["REGION", "TITLE", "LINK", "CONCEPT_TAGS", "CAPACITY_HINT", "PRICE_HINT", "CRAWLED_AT"]]
            .rename(columns={
                "REGION": "지역", "TITLE": "제목", "LINK": "링크", "CONCEPT_TAGS": "컨셉",
                "CAPACITY_HINT": "인원(추정)", "PRICE_HINT": "가격(추정,원)", "CRAWLED_AT": "수집시각",
            }),
            use_container_width=True,
            hide_index=True,
        )

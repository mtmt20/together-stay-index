"""
Together-Stay Index - Step 4: Streamlit 대시보드 (PoC)
======================================================

dbt Gold 레이어(GOLD.MART_KEYWORD_DAILY_TREND, GOLD.MART_CANDIDATE_POSTS)만
읽는 얇은 조회 화면이다. RAW/Silver를 직접 스캔하지 않는 이유: 대시보드를
심사위원이 여러 번 새로고침해도 무거운 집계 쿼리가 매번 웨어하우스를
깨우지 않도록, 무거운 집계는 이미 dbt가 끝내놓고 여기서는 가벼운 SELECT만
한다.

같은 이유로 쿼리 결과는 st.cache_data(ttl=...)로 캐싱한다. 캐시된 동안은
버튼을 눌러도 실제 Snowflake 호출이 나가지 않는다 - 트라이얼 크레딧
환경에서 resume 횟수를 줄이기 위한 핵심 장치.
"""

from __future__ import annotations

import json
import os

import pandas as pd
import snowflake.connector
import streamlit as st
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

st.set_page_config(page_title="Together-Stay Index", layout="wide")

# 쿼리 캐시 유효시간(초). 대시보드를 아무리 새로고침해도 이 시간 안에는
# Snowflake 웨어하우스를 다시 깨우지 않는다. 데모 직전에만 짧게 낮추면 된다.
CACHE_TTL_SECONDS = 3600


# secrets.toml이 아예 없는 로컬 Docker 환경에서는 st.secrets를 건드리기만
# 해도 Streamlit이 "No secrets found" 경고 배너를 화면에 계속 찍어낸다
# (try/except로도 안 막힘 - Streamlit 런타임이 자체적으로 렌더링하는
# 것으로 보임). 그래서 st.secrets를 아예 만지기 전에 파일 존재 여부부터
# 확인해서, 파일이 있을 때만(=Streamlit Cloud) 접근한다.
_SECRETS_FILE_EXISTS = any(
    os.path.exists(p)
    for p in (
        "/app/.streamlit/secrets.toml",
        os.path.expanduser("~/.streamlit/secrets.toml"),
        ".streamlit/secrets.toml",
    )
)


def _config(key: str, default: str | None = None) -> str | None:
    """설정값을 두 군데에서 찾는다: 로컬 Docker는 환경변수, Streamlit
    Community Cloud는 st.secrets(대시보드 설정 화면에 입력한 값)를 쓴다."""
    if _SECRETS_FILE_EXISTS:
        try:
            if key in st.secrets:
                return st.secrets[key]
        except Exception:  # noqa: BLE001 - 혹시 모를 파싱 에러 등
            pass
    return os.environ.get(key, default)


def _load_private_key_der() -> bytes:
    """이 계정은 비밀번호가 아니라 키페어 인증을 쓴다. PEM(PKCS8) 개인키를
    읽어서 snowflake-connector-python이 요구하는 DER 바이트로 변환한다.

    로컬 Docker에서는 마운트된 파일(SNOWFLAKE_PRIVATE_KEY_PATH)을 읽고,
    Streamlit Community Cloud에는 파일을 올릴 수 없으므로 개인키 PEM 텍스트
    자체를 secret(SNOWFLAKE_PRIVATE_KEY)로 붙여넣게 한다."""
    passphrase = _config("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE") or None
    pem_text = _config("SNOWFLAKE_PRIVATE_KEY")
    if pem_text:
        pem_bytes = pem_text.encode()
    else:
        with open(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"], "rb") as f:
            pem_bytes = f.read()

    private_key = serialization.load_pem_private_key(
        pem_bytes,
        password=passphrase.encode() if passphrase else None,
        backend=default_backend(),
    )
    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


@st.cache_resource
def get_connection() -> snowflake.connector.SnowflakeConnection:
    """Snowflake 커넥션은 세션당 한 번만 생성해 재사용한다."""
    return snowflake.connector.connect(
        account=_config("SNOWFLAKE_ACCOUNT"),
        user=_config("SNOWFLAKE_USER"),
        private_key=_load_private_key_der(),
        role=_config("SNOWFLAKE_ROLE", "SYSADMIN"),
        warehouse=_config("SNOWFLAKE_WAREHOUSE"),
        database="TOGETHER_STAY_DB",
        schema="GOLD",
    )


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_keyword_trend() -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql(
        "select KEYWORD, CRAWLED_DATE, MENTION_COUNT, UNIQUE_POST_COUNT "
        "from MART_KEYWORD_DAILY_TREND order by CRAWLED_DATE",
        conn,
    )


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_candidate_posts() -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql(
        "select KEYWORD, SOURCE, TITLE, LINK, CRAWLED_AT, "
        "MENTIONS_BATHROOM, MENTIONS_YARD, MENTIONS_WHOLE_HOUSE "
        "from MART_CANDIDATE_POSTS order by CRAWLED_AT desc",
        conn,
    )


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_food_cafe() -> pd.DataFrame:
    conn = get_connection()
    df = pd.read_sql(
        "select CATEGORY, REGION, TITLE, LINK, SOURCE, CRAWLED_AT, "
        "MENTIONS_REVISIT, MENTIONS_KIDS_FRIENDLY, MENTIONS_NATURE, "
        "CAPACITY_HINT, PRICE_HINT, CONCEPT_TAGS, RECOMMEND_SCORE "
        "from MART_FOOD_CAFE order by REGION, CATEGORY, RECOMMEND_SCORE desc",
        conn,
    )
    # CONCEPT_TAGS는 Snowflake ARRAY라 JSON 문자열로 온다 - 화면 표시용으로 풀어준다.
    if not df.empty:
        df["CONCEPT_TAGS"] = df["CONCEPT_TAGS"].apply(
            lambda v: ", ".join(json.loads(v)) if v else ""
        )
    return df


st.title("Together-Stay Index")
st.caption("다둥이 / 다가구 연합 체류형 숙소 수요 모니터링 대시보드 (PoC)")

col1, col2 = st.columns([3, 1])
with col2:
    if st.button("데이터 새로고침 (캐시 초기화)"):
        st.cache_data.clear()
        st.rerun()

try:
    trend_df = load_keyword_trend()
    posts_df = load_candidate_posts()
    food_cafe_df = load_food_cafe()
except Exception as exc:  # noqa: BLE001 - 대시보드 최상단에서 원인을 그대로 보여주기 위함
    st.error(
        "Snowflake 연결/조회에 실패했습니다. 환경변수(SNOWFLAKE_*)와 "
        "dbt run이 먼저 수행되었는지 확인하세요."
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

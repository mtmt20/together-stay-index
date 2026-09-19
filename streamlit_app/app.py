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

import html
import os
import tempfile

import duckdb
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Together-Stay Index", page_icon="🏡", layout="wide")

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


STYLE = """
<style>
@import url('https://cdn.jsdelivr.net/gh/orioncactus/pretendard@v1.3.9/dist/web/static/pretendard.min.css');
:root { --ink:#0f172a; --muted:#64748b; --card:#ffffff; --line:#e8ecf3;
        --a:#6366f1; --b:#ec4899; --c:#f59e0b; --bg:#f6f7fb; }
html, body, [class*="css"], .stApp { font-family:'Pretendard',-apple-system,sans-serif; }
.stApp { background: var(--bg); }
.block-container { padding-top:2rem; max-width:1200px; }
header[data-testid="stHeader"] { background:transparent; }
.hero { position:relative; overflow:hidden; border-radius:28px; padding:44px 48px; color:#fff;
  background:linear-gradient(120deg,#4f46e5 0%,#7c3aed 45%,#ec4899 100%);
  box-shadow:0 20px 50px -20px rgba(99,102,241,.55); margin-bottom:28px; }
.hero:after { content:""; position:absolute; right:-80px; top:-80px; width:320px; height:320px;
  border-radius:50%; background:radial-gradient(circle,rgba(255,255,255,.28),transparent 70%); }
.hero h1 { font-size:2.6rem; font-weight:800; letter-spacing:-.03em; margin:0 0 8px; color:#fff; padding:0; }
.hero p { font-size:1.05rem; opacity:.92; margin:0 0 18px; color:#fff; }
.chip { display:inline-block; padding:5px 12px; margin:0 6px 6px 0; border-radius:999px;
  font-size:.78rem; font-weight:600; background:rgba(255,255,255,.18); backdrop-filter:blur(6px); }
.kpis { display:grid; grid-template-columns:repeat(3,1fr); gap:16px; margin-bottom:8px; }
.kpi { background:var(--card); border:1px solid var(--line); border-radius:20px; padding:22px 24px;
  box-shadow:0 8px 24px -16px rgba(15,23,42,.18); }
.kpi .l { color:var(--muted); font-size:.85rem; font-weight:600; }
.kpi .v { font-size:2.2rem; font-weight:800; letter-spacing:-.03em; color:var(--a);
  background:linear-gradient(120deg,var(--a),var(--b)); -webkit-background-clip:text;
  -webkit-text-fill-color:transparent; }
.sec { margin:34px 0 4px; font-size:1.45rem; font-weight:800; letter-spacing:-.02em; color:var(--ink); }
.sub { color:var(--muted); font-size:.92rem; margin-bottom:14px; }
.grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(300px,1fr)); gap:16px; margin-top:8px; }
.card { background:var(--card); border:1px solid var(--line); border-radius:20px; padding:20px;
  transition:transform .15s, box-shadow .15s; display:flex; flex-direction:column; gap:10px; }
.card:hover { transform:translateY(-3px); box-shadow:0 18px 36px -20px rgba(99,102,241,.5); }
.card .rg { align-self:flex-start; font-size:.74rem; font-weight:700; color:var(--a);
  background:#eef0ff; padding:4px 10px; border-radius:999px; }
.card a { color:var(--ink); font-weight:700; font-size:.98rem; line-height:1.4; text-decoration:none; }
.card a:hover { color:var(--a); }
.tag { display:inline-block; font-size:.72rem; font-weight:600; padding:3px 9px; margin:0 5px 0 0;
  border-radius:8px; background:#fdf2f8; color:#be185d; }
.tag.g { background:#ecfdf5; color:#047857; } .tag.y { background:#fffbeb; color:#b45309; }
.meta { color:var(--muted); font-size:.78rem; margin-top:auto; }
button[data-baseweb="tab"] { font-weight:700; }
div[data-testid="stDataFrame"] { border-radius:16px; overflow:hidden; border:1px solid var(--line); }
@media (max-width:720px){ .kpis{grid-template-columns:1fr;} .hero{padding:30px 24px;} .hero h1{font-size:2rem;} }
</style>
"""
st.markdown(STYLE, unsafe_allow_html=True)

st.markdown(
    """
<div class="hero">
  <h1>Together-Stay Index</h1>
  <p>다둥이 · 다가구 함께 떠나는 여행, 숙소부터 맛집·카페까지 한눈에</p>
  <span class="chip">🏡 독채·마당</span><span class="chip">👨‍👩‍👧‍👦 두 가족</span>
  <span class="chip">🍽️ 재방문 맛집</span><span class="chip">☕ 노키즈존 없는 카페</span>
</div>
""",
    unsafe_allow_html=True,
)

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

mentions = int(trend_df["MENTION_COUNT"].sum()) if not trend_df.empty else 0
uniques = int(trend_df["UNIQUE_POST_COUNT"].sum()) if not trend_df.empty else 0
st.markdown(
    f"""
<div class="kpis">
  <div class="kpi"><div class="l">누적 언급 글</div><div class="v">{mentions:,}</div></div>
  <div class="kpi"><div class="l">고유 글(URL)</div><div class="v">{uniques:,}</div></div>
  <div class="kpi"><div class="l">본문 확보 후보 글</div><div class="v">{len(posts_df):,}</div></div>
</div>
""",
    unsafe_allow_html=True,
)

_, refresh_col = st.columns([5, 1])
with refresh_col:
    if st.button("🔄 새로고침", use_container_width=True):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()

# --- 트렌드 ------------------------------------------------------------
st.markdown('<div class="sec">📈 키워드별 언급 추이</div>', unsafe_allow_html=True)
if trend_df.empty:
    st.info("아직 적재된 데이터가 없습니다. Airflow DAG와 dbt run을 먼저 실행하세요.")
else:
    pivot = trend_df.pivot(index="CRAWLED_DATE", columns="KEYWORD", values="MENTION_COUNT").fillna(0)
    st.area_chart(pivot, height=260)

# --- 숙소 후보 ---------------------------------------------------------
st.markdown('<div class="sec">🏡 숙소 후보 언급 글</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="sub">블로그·카페에서 수집한 실제 후기. 화장실·마당·독채 언급 여부를 표시했어요.</div>',
    unsafe_allow_html=True,
)

available_keywords = sorted(posts_df["KEYWORD"].unique().tolist()) if not posts_df.empty else []
f1, f2 = st.columns([4, 2])
with f1:
    selected_keywords = st.multiselect("키워드", available_keywords, default=available_keywords)
with f2:
    only_with_signal = st.checkbox("화장실/마당/독채 언급 글만", value=False)

filtered = posts_df[posts_df["KEYWORD"].isin(selected_keywords)] if selected_keywords else posts_df
if only_with_signal:
    filtered = filtered[
        filtered["MENTIONS_BATHROOM"] | filtered["MENTIONS_YARD"] | filtered["MENTIONS_WHOLE_HOUSE"]
    ]

st.dataframe(
    filtered[["KEYWORD", "TITLE", "LINK", "MENTIONS_BATHROOM", "MENTIONS_YARD", "MENTIONS_WHOLE_HOUSE", "CRAWLED_AT"]],
    use_container_width=True,
    hide_index=True,
    column_config={
        "KEYWORD": "키워드",
        "TITLE": st.column_config.TextColumn("제목", width="large"),
        "LINK": st.column_config.LinkColumn("링크", display_text="열기 ↗"),
        "MENTIONS_BATHROOM": st.column_config.CheckboxColumn("🚻 화장실"),
        "MENTIONS_YARD": st.column_config.CheckboxColumn("🌳 마당"),
        "MENTIONS_WHOLE_HOUSE": st.column_config.CheckboxColumn("🏠 독채"),
        "CRAWLED_AT": st.column_config.DatetimeColumn("수집", format="MM/DD HH:mm"),
    },
)

# --- 맛집/카페 ---------------------------------------------------------
st.markdown('<div class="sec">🍽️ 지역별 갈만한 맛집 · 카페</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="sub">맛집은 재방문 언급이 많은 곳부터, 카페는 노키즈존이 아니면서 '
    "자연·아동친화 요소가 있는 곳만 모았어요.</div>",
    unsafe_allow_html=True,
)

TAG_CLASS = {"재방문많음": "", "아동친화": "g", "자연/뷰": "y"}


def _card(row) -> str:
    tags = "".join(
        f'<span class="tag {TAG_CLASS.get(t, "")}">{html.escape(t)}</span>'
        for t in [t.strip() for t in str(row["CONCEPT_TAGS"]).split(",")]
        if t
    )
    meta = []
    if pd.notna(row["CAPACITY_HINT"]):
        meta.append(f"👥 {int(row['CAPACITY_HINT'])}명")
    if pd.notna(row["PRICE_HINT"]):
        meta.append(f"💰 {int(row['PRICE_HINT']):,}원")
    link = html.escape(str(row["LINK"]), quote=True)
    return (
        f'<div class="card"><span class="rg">{html.escape(str(row["REGION"]))}</span>'
        f'<a href="{link}" target="_blank" rel="noopener">{html.escape(str(row["TITLE"]))}</a>'
        f'<div>{tags}</div><div class="meta">{" · ".join(meta) or "&nbsp;"}</div></div>'
    )


if food_cafe_df.empty:
    st.info("아직 맛집/카페 데이터가 없습니다. food_cafe_crawler DAG 태스크를 먼저 실행하세요.")
else:
    region_options = sorted(food_cafe_df["REGION"].unique().tolist())
    selected_regions = st.multiselect("지역", region_options, default=region_options)
    fc_filtered = food_cafe_df[food_cafe_df["REGION"].isin(selected_regions)]

    food_tab, cafe_tab = st.tabs(["🍽️ 맛집 · 재방문순", "☕ 카페 · 자연/아동친화"])
    for tab, cat in ((food_tab, "food"), (cafe_tab, "cafe")):
        with tab:
            view = fc_filtered[fc_filtered["CATEGORY"] == cat].sort_values("RECOMMEND_SCORE", ascending=False)
            if view.empty:
                st.caption("조건에 맞는 곳이 없어요.")
                continue
            st.markdown(
                '<div class="grid">' + "".join(_card(r) for _, r in view.head(18).iterrows()) + "</div>",
                unsafe_allow_html=True,
            )
            if len(view) > 18:
                with st.expander(f"전체 {len(view)}곳 표로 보기"):
                    st.dataframe(
                        view[["REGION", "TITLE", "LINK", "CONCEPT_TAGS", "CAPACITY_HINT", "PRICE_HINT"]],
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "REGION": "지역",
                            "TITLE": "제목",
                            "CONCEPT_TAGS": "컨셉",
                            "LINK": st.column_config.LinkColumn("링크", display_text="열기 ↗"),
                            "CAPACITY_HINT": "인원(추정)",
                            "PRICE_HINT": "가격(추정,원)",
                        },
                    )

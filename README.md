# Together-Stay Index (Step 1~4 뼈대)

공공데이터 활용 경진대회용 "다둥이/다가구 연합 체류형 관광 최적화 대시보드" 파이프라인.
크롤링(S3) -> Airflow -> dbt(DuckDB, Silver/Gold) -> Streamlit 전 구간 뼈대가 갖춰져 있다.

2026-09-18: Snowflake에서 DuckDB로 이전했다. DuckDB는 서버가 아니라 파일
하나짜리 엔진이라 계정/웨어하우스/키페어가 전혀 필요 없고, 완전 무료다.
dbt가 S3 Bronze의 parquet 파일을 httpfs로 직접 읽어(`read_parquet`) Silver/
Gold를 만들고, 그 결과가 담긴 `.duckdb` 파일을 로컬(Docker 볼륨)과
Streamlit Community Cloud(S3 스냅샷) 양쪽에서 그대로 읽는다.

## 기존 프로젝트와의 충돌 방지 체크리스트

이 프로젝트는 `D:/00.py/whisky_hot` 등 기존에 돌고 있는 프로젝트와 완전히
분리된 새 디렉토리(`D:/00.py/together_stay`)이며, 아래를 전부 새로 만들어
써야 충돌이 없다. 기존 것을 재사용하지 말 것.

| 항목 | 이 프로젝트 값 | 기존(위스키) 값 | 비고 |
|---|---|---|---|
| Airflow 웹서버 포트 | 8090 | 8080 | docker-compose.yml에서 회피 처리됨 |
| Streamlit 포트 | 8510 | 8501~8504 | 위스키 Streamlit들이 이미 8501~8504 사용 중이라 회피 |
| Postgres 포트 | 55432 | (내부용) | 호스트 노출 포트만 회피 |
| Docker 컨테이너명 | `tsi_*` | 별도 | 접두어로 구분 |
| Docker 네트워크/볼륨 | `tsi_network`, `tsi_postgres_data` | 별도 | 이름 고유화 |
| S3 버킷 | **새로 생성** (예: `together-stay-index-bronze`) | 기존 버킷 | `.env`에서 지정, 재사용 금지 |
| 데이터 웨어하우스 | DuckDB(`./warehouse/together_stay.duckdb`, 로컬 파일) | Snowflake | 계정 공유 없음, 완전 분리 |
| Docker 이미지 | 자체 Dockerfile로 신규 빌드 | 기존 커스텀 이미지 | 이미지 레이어까지 분리 |

## 최초 1회 설정

1. `.env.example`을 `.env`로 복사하고 네이버 API 키 / AWS 키 / 신규 S3 버킷명을 채운다.
2. 이 프로젝트 전용 S3 버킷을 새로 만든다 (`aws s3 mb s3://together-stay-index-bronze`).
3. `docker compose up -d` 로 이 프로젝트 전용 Airflow를 띄운다.
4. `http://localhost:8090` (admin/admin) 접속 후 DAG
   `together_stay_index_naver_ingestion`을 Unpause 한다.

## Step 3: dbt 실행 (Silver/Gold)

DAG가 하루치 Bronze parquet을 S3에 쌓으면, 수동으로(또는 나중에 DAG에
태스크로 추가해) dbt를 돌려 Silver(`stg_naver_posts`, `stg_food_cafe_posts`)와
Gold(`mart_keyword_daily_trend`, `mart_candidate_posts`, `mart_food_cafe`)를
만든다. DuckDB는 비용이 없으므로 원하는 만큼 자주 돌려도 된다.

```bash
docker compose run --rm tsi-dbt
```

`tsi-dbt`는 `docker compose up`에는 포함되지 않는 "tools" 프로필이다.
`dbt run`이 끝나면 같은 컨테이너가 이어서 `publish_to_s3.py`를 실행해
결과 `.duckdb` 파일을 S3(`gold-warehouse/together_stay.duckdb`)에도
올려둔다 - Streamlit Community Cloud가 이 스냅샷을 내려받아 쓴다.

## Step 4: Streamlit 대시보드

```bash
docker compose up -d tsi-streamlit
```

`http://localhost:8510` 에서 키워드별 언급 추이(`mart_keyword_daily_trend`),
본문이 확보된 숙소 후보 글 목록(`mart_candidate_posts`), 지역별 맛집/카페
큐레이션(`mart_food_cafe`)을 확인할 수 있다. 로컬 Docker에서는 tsi-dbt와
같은 볼륨(`./warehouse`)을 마운트해 파일을 바로 읽는다. 쿼리는 1시간
캐시(`st.cache_data(ttl=3600)`)되므로 "데이터 새로고침" 버튼을 눌러야
최신 dbt 결과가 반영된다.

Streamlit Community Cloud에 배포할 때는 secrets에 `AWS_ACCESS_KEY_ID` /
`AWS_SECRET_ACCESS_KEY` / `AWS_S3_BUCKET` / `AWS_REGION`만 넣으면 된다
(Snowflake 키페어 같은 복잡한 값이 없어졌다) - 앱이 S3의 최신 `.duckdb`
스냅샷을 내려받아서 연다.

## 참고: 비용

DuckDB는 서버가 없는 임베디드 엔진이라 Snowflake처럼 "웨어하우스를 몇 번
깨우는지"로 과금되는 개념 자체가 없다. 실질적인 비용은 S3 저장/전송량
정도로, 이 프로젝트 규모에서는 사실상 무시할 수준이다.

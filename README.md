# Together-Stay Index (Step 1~4 뼈대)

공공데이터 활용 경진대회용 "다둥이/다가구 연합 체류형 관광 최적화 대시보드" 파이프라인.
크롤링(S3) -> Airflow -> Snowflake Raw -> dbt(Silver/Gold) -> Streamlit 전 구간 뼈대가 갖춰져 있다.

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
| Snowflake DB/스키마 | `TOGETHER_STAY_DB.RAW` | 기존 DB | `dags/sql/snowflake_setup.sql` |
| Airflow Connection id | `together_stay_snowflake` | 기존 conn id | 새 Connection으로 등록 |
| Docker 이미지 | 자체 Dockerfile로 신규 빌드 | 기존 커스텀 이미지 | 이미지 레이어까지 분리 |

## 최초 1회 설정

1. `.env.example`을 `.env`로 복사하고 네이버 API 키 / AWS 키 / 신규 S3 버킷명을 채운다.
2. 이 프로젝트 전용 S3 버킷을 새로 만든다 (`aws s3 mb s3://together-stay-index-bronze`).
3. Snowflake에서 `dags/sql/snowflake_setup.sql`을 한 번 실행해 DB/스테이지/RAW 테이블을 만든다
   (Storage Integration은 이 버킷만 바라보도록 별도로 새로 만들 것).
4. `docker compose up -d` 로 이 프로젝트 전용 Airflow를 띄운다.
5. `http://localhost:8090` (admin/admin) 접속 후 Admin > Connections에서
   `together_stay_snowflake` Connection을 등록한다.
6. DAG `together_stay_index_naver_ingestion`을 Unpause 한다.

## Step 3: dbt 실행 (Silver/Gold)

DAG가 하루치 RAW 데이터를 COPY INTO 한 뒤, 수동으로(또는 나중에 DAG에
태스크로 추가해) dbt를 돌려 Silver(`stg_naver_posts`)와
Gold(`mart_keyword_daily_trend`, `mart_candidate_posts`)를 만든다.

```bash
docker compose run --rm tsi-dbt run
```

`tsi-dbt`는 `docker compose up`에는 포함되지 않는 "tools" 프로필이다.
dbt run 자체가 Snowflake 웨어하우스를 깨우는 행위이므로, 필요할 때만
명시적으로 실행하고 상시 컨테이너로 띄워두지 않는다.

## Step 4: Streamlit 대시보드

```bash
docker compose up -d tsi-streamlit
```

`http://localhost:8510` 에서 키워드별 언급 추이(`mart_keyword_daily_trend`)와
본문이 확보된 숙소 후보 글 목록(`mart_candidate_posts`)을 확인할 수 있다.
쿼리는 1시간 캐시(`st.cache_data(ttl=3600)`)되므로 화면을 새로고침해도
매번 Snowflake를 깨우지 않는다. 최신 데이터를 강제로 보려면 화면의
"데이터 새로고침" 버튼을 누른다.

## 참고: 웨어하우스 비용

Snowflake 웨어하우스는 resume 1회당 최소 60초가 과금되므로, DDL/스테이지
변경 같은 1회성 작업은 `snowflake_setup.sql`처럼 매일 도는 DAG와 분리해
필요할 때만 수동으로 실행한다.

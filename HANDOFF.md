# Together-Stay Index 작업 인수인계

이 파일은 컴퓨터 세션 ↔ 폰 세션처럼 서로 다른 Claude Code 세션이 이어서 작업할 때 쓰는
공유 메모입니다. **작업을 시작하기 전에 이 파일을 먼저 읽고, 의미 있는 작업을 끝내면
이 파일을 업데이트**해주세요 (아래 "최근 업데이트"를 맨 위에 추가하고, 필요하면 "진행 중 /
남은 작업"도 고쳐주세요). 대화 맥락은 세션마다 다르지만 이 파일은 디스크에 있어서 항상
공유됩니다.

## 현재 실행 중 (세션 시작 시 가장 먼저 확인 - `docker compose ps`로 실제 상태도 같이 확인할 것)

**(2026-09-16 기준) 없음.** 아직 `docker compose up`을 한 번도 실행한 적 없음
(이미지 빌드까지만 완료됨). 이 줄을 안 고치고 세션이 뭔가를 띄웠다면 여기 업데이트할 것 -
예: "2026-09-16 밤, 데스크톱 세션이 `docker compose up -d` 실행 중, 끝내려면
`docker compose down`".

## 프로젝트 개요

공공데이터 활용 경진대회(마감 2026-09-30) 출품작. "다둥이(3명 이상 자녀) 가구 /
두 가족 이상 연합" 여행객이 눈치 안 보고 묵을 수 있는 독채 숙소 + 주변 인프라를
큐레이션하는 대시보드. 스택: 크롤러(Python) → S3(Bronze) → Airflow(Docker) →
Snowflake(Raw) → dbt(Silver/Gold) → Streamlit.

**기존 위스키 파이프라인(`D:/00.py/whisky_hot`)과는 완전히 분리된 독립 프로젝트/독립
git 저장소**다. 포트, Docker 컨테이너명/네트워크/볼륨, S3 버킷, Snowflake DB, Airflow
Connection id를 전부 새로 만들어서 절대 안 겹치게 설계했다 (자세한 건 [README.md](README.md)의
"기존 프로젝트와의 충돌 방지 체크리스트" 표 참고).

## 최근 업데이트 (최신이 위로)

### 2026-09-17, 데스크톱 - Snowflake 키페어 인증으로 전환 (계정 재사용 확정)
사용자가 "위스키 쪽 Snowflake 크레딧이 240 정도 남았고 널널하니 새 계정 안 파고 그냥
이 계정 재사용하자"고 결정함. 그런데 위스키의 Snowflake 연결이 **비밀번호가 아니라
키페어(RSA 개인키) 인증**이라는 걸 `whisky-pipeline/docker-compose.yml`에서 확인함
(계정 `svgjcjl-uh88149`, user `mtmt20`, warehouse `WHISKY_WH`, role `ACCOUNTADMIN`).

- 위스키가 쓰는 개인키 파일(`C:/Users/Administrator/.dbt/rsa_key.p8`)을 그대로
  마운트해서 공유하려 했으나, Claude Code의 auto-mode 안전 분류기가 "Credential
  Leakage"/"Secret-Store Writes"로 자동 차단함 (다른 프로젝트에 기존 키 파일 접근권을
  그대로 넘기는 걸 막는 정상 동작). 우회하지 않고 **이 프로젝트 전용 키를 새로 발급**하는
  쪽으로 방향 전환.
- `openssl`로 새 RSA 키페어 생성 → [secrets/rsa_key.p8](secrets/rsa_key.p8) (개인키,
  `.gitignore`에 추가돼 커밋 안 됨) / `secrets/rsa_key.pub` (공개키).
- 같은 유저 `mtmt20`에 **두 번째 공개키**로 등록하는 방식 사용 (Snowflake는 유저당 키
  2개까지 동시 허용 - `RSA_PUBLIC_KEY_2`). 위스키가 쓰는 첫 번째 키는 전혀 안 건드림.
  **아직 사용자가 Snowflake 워크시트에서 `ALTER USER mtmt20 SET RSA_PUBLIC_KEY_2=...`
  SQL을 실행 안 함 - 다음 세션에서 이거 됐는지부터 확인할 것** (공개키 값은 이 대화
  로그에 있음, 필요하면 `secrets/rsa_key.pub`에서 다시 뽑으면 됨).
- `docker-compose.yml`(airflow-common + tsi-streamlit + tsi-dbt 전부), `dbt/profiles.yml`,
  `dbt/profiles.yml.example`, `streamlit_app/app.py`(PEM→DER 변환 로직 추가),
  `streamlit_app/requirements.txt`(cryptography 추가), `.env.example`을 전부 비밀번호
  방식에서 `private_key_path` 방식으로 고침. SNOWFLAKE_ACCOUNT/USER/WAREHOUSE/ROLE은
  위스키 값을 기본값으로 docker-compose.yml에 박아둬서 `.env`에 안 채워도 동작하게 함.
- `tsi-airflow-init`이 `together_stay_snowflake` Airflow Connection을 **자동으로
  등록**하도록 명령어 추가함 (위스키가 하던 방식과 동일) - 예전 HANDOFF에 있던 "Admin
  UI에서 수동 등록" 단계는 이제 필요 없음.
- `docker compose config`로 문법 검증 통과. `tsi-streamlit` 이미지 재빌드 진행 중
  (cryptography 의존성 추가 때문) - 다음 세션에서 빌드 성공했는지 확인할 것.

### 2026-09-15, 데스크톱 - Step 1~4 뼈대 완성 + 빌드 검증 + git init
- Step 1 크롤러([dags/crawler/naver_crawler.py](dags/crawler/naver_crawler.py)): 네이버 검색
  오픈API(blog/cafearticle)로 키워드 검색 → BeautifulSoup으로 본문 보강 → Parquet 변환 →
  S3 업로드.
- Step 2 DAG([dags/together_stay_dag.py](dags/together_stay_dag.py)): 매일 크롤링→S3 업로드
  후 Snowflake RAW로 COPY INTO.
- Step 3 dbt([dbt/](dbt/)): stg_naver_posts(Silver) → mart_keyword_daily_trend,
  mart_candidate_posts(Gold).
- Step 4 Streamlit([streamlit_app/app.py](streamlit_app/app.py)): Gold만 읽는 대시보드,
  1시간 캐시로 Snowflake 웨어하우스 재가동 최소화.
- **빌드 버그 발견/수정**: 처음엔 `requirements.txt`에 pandas/boto3/pyarrow 버전을 세게
  고정해놨더니 `apache-airflow-providers-snowflake`의 의존성과 충돌해서 pip가 실패
  (첫 시도) → 무한 백트래킹(ResolutionTooDeep, 두 번째 시도)까지 났음. Airflow 공식
  constraints 파일(`--constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt`)을
  Dockerfile에 적용하고 겹치는 패키지 버전 고정을 풀어서 해결. **5개 이미지
  (airflow-init/webserver/scheduler, streamlit, dbt) 전부 실제로 빌드 성공 확인함**
  (`docker compose build`, `docker compose --profile tools build tsi-dbt`).
- 독립 git 저장소로 `git init` + 커밋 2개 완료 (위스키 repo와 무관).
- Python 문법은 `py_compile`로 크롤러/DAG/Streamlit 앱 전부 확인함.
- **아직 실행 자체는 검증 안 됨** - 아래 "남은 작업" 채워야 `docker compose up`부터
  실제로 돌려볼 수 있음.

## 진행 중 / 남은 작업

Snowflake 계정/DB명은 이제 코드 기본값으로 다 채워져 있어서(위스키 계정 재사용,
`.env`에 안 넣어도 됨) 실제로 사용자가 해야 하는 건 아래 4개뿐:

1. **[아직 안 함] Snowflake 워크시트에서 새 공개키 등록 SQL 1회 실행** -
   `ALTER USER mtmt20 SET RSA_PUBLIC_KEY_2='...';` (공개키 값은 2026-09-17 업데이트
   항목 참고, 또는 `secrets/rsa_key.pub` 파일에서 BEGIN/END 줄 빼고 한 줄로 이어붙이면
   나옴). **이거 먼저 해야 이 프로젝트가 Snowflake에 붙을 수 있음.**
2. 네이버 오픈API 키 발급 (developers.naver.com/apps/#/register → 검색 API) → `.env`의
   `NAVER_CLIENT_ID`/`NAVER_CLIENT_SECRET`
3. 이 프로젝트 전용 S3 버킷 신규 생성 (예: `together-stay-index-bronze`, 기존 위스키
   버킷 재사용 금지) → `.env`의 `AWS_*`. Snowflake Storage Integration도 이 버킷
   전용으로 새로 생성 필요.
4. 1번 SQL 실행 후, [dags/sql/snowflake_setup.sql](dags/sql/snowflake_setup.sql)을
   Snowflake 워크시트에서 1회 실행 (TOGETHER_STAY_DB/스테이지/RAW 테이블 생성)

위 4개 끝나면: `docker compose up -d` (Airflow Connection은 이제 자동 등록됨 - 수동
UI 등록 단계 없음) → DAG Unpause → 1회 실행 확인 → `docker compose run --rm tsi-dbt run`
→ `http://localhost:8510`에서 대시보드 확인. 아직 실제 크롤링/COPY INTO/dbt run이
한 번도 돈 적 없으니, 다음 세션에서 이 순서를 처음부터 검증해야 함.

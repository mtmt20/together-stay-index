# Together-Stay Index 작업 인수인계

이 파일은 컴퓨터 세션 ↔ 폰 세션처럼 서로 다른 Claude Code 세션이 이어서 작업할 때 쓰는
공유 메모입니다. **작업을 시작하기 전에 이 파일을 먼저 읽고, 의미 있는 작업을 끝내면
이 파일을 업데이트**해주세요 (아래 "최근 업데이트"를 맨 위에 추가하고, 필요하면 "진행 중 /
남은 작업"도 고쳐주세요). 대화 맥락은 세션마다 다르지만 이 파일은 디스크에 있어서 항상
공유됩니다.

## 현재 실행 중 (세션 시작 시 가장 먼저 확인 - `docker compose ps`로 실제 상태도 같이 확인할 것)

**(2026-09-17 기준) `docker compose up -d`로 5개 컨테이너 전부 떠 있음** (webserver
헬스체크 통과 확인됨, http://localhost:8090). 안 쓸 때는 `docker compose stop`으로
내려두는 걸 권장 (아래 자원 이슈 참고 - 위스키랑 같이 돌리는 거라 필요할 때만 켜기).

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

### 2026-09-17 밤, 데스크톱 - Snowflake 초기 세팅 완료 + 스택 최초 기동 + 호스트 메모리 부족 발견/해결
- 사용자가 준 Naver 없이 진행 가능한 것들부터: AWS 콘솔에서 `together-stay-index-bronze`
  버킷 생성, 전용 IAM 사용자 `together-stay-index-uploader`(AmazonS3FullAccess) +
  액세스 키 발급, `.env`에 기입. Snowflake는 `ALTER USER mtmt20 SET RSA_PUBLIC_KEY_2=...`
  사용자가 직접 실행 완료 확인.
- `dags/sql/snowflake_setup.sql`을 그대로 쓰지 않고, scratchpad에 임시 Python
  스크립트(snowflake-connector-python + 키페어)를 짜서 **DB/스키마/스테이지/테이블을
  Claude가 직접 실행해서 생성함** (STORAGE INTEGRATION 대신 스테이지에 AWS
  액세스키를 직접 CREDENTIALS로 넣는 방식으로 단순화 - IAM Role 신뢰관계 설정 안
  거쳐도 됨). 4개 문 전부 OK 확인.
- `docker compose up -d`로 **이 프로젝트 최초 기동**. 처음엔 `tsi-airflow-webserver`/
  `scheduler`가 `depends_on: tsi-airflow-init`이 "시작"만 기다리고 "완료"는 안
  기다려서, `airflow db migrate`가 끝나기 전에 떠버려 "DB 초기화 안 됨" 에러로 죽는
  경합조건 발견 → `condition: service_completed_successfully`로 수정.
- **더 큰 문제**: 위스키 스택(6개 컨테이너)이랑 이 프로젝트 스택(5개)이 이 컴퓨터에서
  동시에 도니, Docker(WSL2)에 할당된 메모리가 7.7GB뿐이라(실물 15.9GB인데
  `.wslconfig`가 없어서 기본값인 절반만 씀) 부족해서 `tsi-airflow-webserver`의
  gunicorn이 "No response from gunicorn master within 120 seconds"로 죽음
  (위스키 HANDOFF에 있던 그 크래시 패턴과 동일 증상 - 원인은 pid 파일이 아니라
  이번엔 메모리 부족이었음). `docker stats`/`docker compose stop`조차 응답 안 할
  정도로 호스트가 눌려있었음.
  - **해결 1**: `C:\Users\Administrator\.wslconfig`에 `memory=12GB` 설정 →
    `wsl --shutdown`으로 재시작 (위스키 컨테이너들도 같이 내려갔다가 restart
    정책 덕에 자동으로 다시 살아남, 약 40초 걸림). Docker 메모리 한도
    7.7GB → 11.68GB로 확인됨.
  - **해결 2**: `docker-compose.yml`의 `AIRFLOW__WEBSERVER__WORKERS`를 기본값
    4에서 `1`로 낮춤 (혼자 쓰는 데모용 웹서버라 4개씩 안 필요함).
  - 적용 후 재기동 확인: 전체 10개 컨테이너(위스키6 + tsi4, streamlit/postgres
    포함하면 tsi5) 합쳐서 메모리 사용량 **2.7GB / 11.68GB (23%)**로 여유 확보,
    webserver 헬스체크(`http://localhost:8090/health`) 정상 통과.
- **다음 세션 참고**: 이제 두 스택을 동시에 켜놔도 자원 문제는 없어야 함. 그래도
  `together-stay-index`는 24시간 떠있을 필요 없는 데모용이니, 안 쓸 때는
  `docker compose stop`으로 내려두는 습관 권장 (CLAUDE.md에도 명시).

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

**Snowflake/AWS 설정은 다 끝남** (2026-09-17 밤 업데이트 참고):
- ✅ Snowflake 키페어 등록, DB/스키마/스테이지/테이블 생성 완료
- ✅ S3 버킷(`together-stay-index-bronze`) + 전용 IAM 키 발급 + `.env` 기입 완료
- ✅ `docker compose up -d`로 스택 기동 확인, 호스트 메모리 문제도 해결됨

**남은 건 딱 1개, 네이버 오픈API 키뿐**:
1. developers.naver.com/apps/#/register → 검색 API 신청 → 발급된
   `NAVER_CLIENT_ID`/`NAVER_CLIENT_SECRET`을 `.env`에 채우기

네이버 키만 채워지면: DAG(`together_stay_index_naver_ingestion`) Unpause → 1회
실행 확인 → `docker compose run --rm tsi-dbt run` → `http://localhost:8510`에서
대시보드 확인. 아직 실제 크롤링/COPY INTO/dbt run이 한 번도 돈 적 없으니, 다음
세션(네이버 키 받은 후)에서 이 순서를 처음부터 끝까지 검증해야 함.

"""
Together-Stay Index - Step 2: Apache Airflow DAG
=================================================

Step 1 크롤러(crawler.naver_crawler.run)를 매일 실행해 S3 Bronze 레이어에
적재하고, 이어서 그 날 적재된 파일만 Snowflake RAW 테이블로 COPY INTO 하는
2-태스크 DAG.

기존 프로젝트와의 충돌 방지
--------------------------
이 DAG는 위스키 파이프라인 등 기존에 이미 돌고 있는 Airflow 인스턴스에
합치지 않고, 이 프로젝트 전용 docker-compose(별도 포트/네트워크/볼륨)로
독립 실행하는 것을 전제로 한다. 그래도 혹시 같은 Airflow 인스턴스에
올리게 되는 경우를 대비해:
  - DAG id를 "together_stay_index_"로 시작하는 고유한 이름으로 지정
  - Airflow Connection id도 "together_stay_"로 시작하는 고유한 이름을 사용
    (기존 프로젝트가 쓰는 aws_default / snowflake_default 커넥션을
     재사용하지 않는다 - 버킷/계정이 다를 수 있으므로 절대 공유 금지)
  - tags=["together-stay-index"] 로 Airflow UI 목록에서 시각적으로도
    다른 프로젝트와 구분되게 한다.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# Snowflake Provider가 설치되어 있으면 SnowflakeOperator를 쓰는 것이
# Airflow Connection(UI에서 관리되는 비밀번호/계정 정보)을 그대로 활용할 수
# 있어 가장 안전하다. (docker-compose에서 apache-airflow-providers-snowflake
# 를 함께 설치한다.)
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

# 이 프로젝트 전용 커넥션/변수 id. 기존 프로젝트의 커넥션과 절대 겹치지
# 않도록 접두어를 강제한다. Airflow UI > Admin > Connections 에서
# 이 id로 새로 등록해야 한다 (기존 커넥션 재사용 금지).
SNOWFLAKE_CONN_ID = "together_stay_snowflake"

SQL_DIR = Path(__file__).parent / "sql"

default_args = {
    "owner": "together-stay-index",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _crawl_and_upload_to_s3(**context) -> None:
    """crawler.naver_crawler.run()을 호출하는 얇은 wrapper.

    DAG 파일 상단이 아니라 함수 내부에서 import하는 이유: Airflow
    스케줄러가 DAG 파일을 주기적으로 다시 파싱(parse)할 때마다 무거운
    의존성(boto3, requests, bs4)까지 매번 새로 import하지 않도록
    지연 로딩(lazy import)한다. DAG 파싱 속도는 스케줄러 전체 성능에
    영향을 주므로 관례적으로 이렇게 분리한다.
    """
    from crawler.naver_crawler import run

    s3_key = run(with_full_text=True)
    # 다음 태스크(Snowflake COPY INTO)가 어떤 파일을 적재했는지 로그로
    # 남겨 디버깅 시 추적할 수 있게 한다. XCom에도 자동으로 저장된다.
    context["ti"].xcom_push(key="uploaded_s3_key", value=s3_key)


with DAG(
    dag_id="together_stay_index_naver_ingestion",
    description="네이버 카페/블로그에서 다둥이/두가족 숙소 언급을 수집해 S3->Snowflake Raw로 적재",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=False,  # 트라이얼 크레딧 환경에서 과거 날짜를 소급 실행하면
                     # 웨어하우스 resume이 그만큼 반복되므로 반드시 False.
    max_active_runs=1,
    tags=["together-stay-index"],
) as dag:

    crawl_and_upload = PythonOperator(
        task_id="crawl_naver_and_upload_to_s3",
        python_callable=_crawl_and_upload_to_s3,
    )

    copy_s3_to_snowflake_raw = SnowflakeOperator(
        task_id="copy_s3_to_snowflake_raw",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=(SQL_DIR / "copy_into_raw.sql").read_text(encoding="utf-8"),
    )

    crawl_and_upload >> copy_s3_to_snowflake_raw

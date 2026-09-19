"""
Together-Stay Index - Step 2: Apache Airflow DAG
=================================================

Step 1 크롤러(crawler.naver_crawler.run / crawler.food_cafe_crawler.run)를
매일 실행해 S3 Bronze 레이어에 적재하는 DAG.

2026-09-18: Snowflake -> DuckDB 이전. Snowflake COPY INTO 태스크가
사라졌다 - DuckDB는 dbt가 S3 parquet을 httpfs로 직접 읽으므로(dbt/models/
staging/*.sql 참고) 별도 "RAW 테이블 적재" 단계가 필요 없다. dbt run은
비용이 들지 않는 로컬 연산이라 `docker compose run --rm tsi-dbt`로 원할
때 수동 실행하면 된다 (자동화하고 싶으면 이 DAG에 태스크를 추가해도 되지만,
크레딧 걱정이 없어졌으니 굳이 서두를 필요는 없다).

기존 프로젝트와의 충돌 방지
--------------------------
이 DAG는 위스키 파이프라인 등 기존에 이미 돌고 있는 Airflow 인스턴스에
합치지 않고, 이 프로젝트 전용 docker-compose(별도 포트/네트워크/볼륨)로
독립 실행하는 것을 전제로 한다. 그래도 혹시 같은 Airflow 인스턴스에
올리게 되는 경우를 대비해:
  - DAG id를 "together_stay_index_"로 시작하는 고유한 이름으로 지정
  - tags=["together-stay-index"] 로 Airflow UI 목록에서 시각적으로도
    다른 프로젝트와 구분되게 한다.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

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
    context["ti"].xcom_push(key="uploaded_s3_key", value=s3_key)


def _crawl_food_cafe_and_upload_to_s3(**context) -> None:
    """crawler.food_cafe_crawler.run()을 호출하는 얇은 wrapper.

    숙소 크롤링 태스크와 서로 의존관계 없이 독립적으로 돈다 (하나가
    실패해도 다른 하나는 정상 적재되게 하기 위함)."""
    from crawler.food_cafe_crawler import run

    s3_key = run(with_full_text=True)
    context["ti"].xcom_push(key="uploaded_food_cafe_s3_key", value=s3_key)


with DAG(
    dag_id="together_stay_index_naver_ingestion",
    description="네이버 카페/블로그에서 다둥이/두가족 숙소 및 맛집/카페 언급을 수집해 S3 Bronze로 적재",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    tags=["together-stay-index"],
) as dag:

    crawl_and_upload = PythonOperator(
        task_id="crawl_naver_and_upload_to_s3",
        python_callable=_crawl_and_upload_to_s3,
    )

    # 맛집/카페 수집 (숙소 수집과 독립적인 별도 태스크 - 하나가 실패해도
    # 다른 하나는 그대로 성공할 수 있게 서로 의존시키지 않는다)
    crawl_food_cafe_and_upload = PythonOperator(
        task_id="crawl_food_cafe_and_upload_to_s3",
        python_callable=_crawl_food_cafe_and_upload_to_s3,
    )

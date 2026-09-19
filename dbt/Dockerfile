# Together-Stay Index 전용 dbt 실행 이미지.
# DuckDB는 서버가 아니라 파일 기반 엔진이라 별도 계정/웨어하우스가 필요
# 없다 - dbt-duckdb만 설치하면 된다. boto3는 dbt run이 끝난 뒤 결과
# .duckdb 파일을 S3에 올려 Streamlit Cloud가 내려받아 쓸 수 있게 하는
# publish_to_s3.py용.
FROM python:3.11-slim

RUN pip install --no-cache-dir dbt-duckdb==1.8.4 boto3

WORKDIR /dbt

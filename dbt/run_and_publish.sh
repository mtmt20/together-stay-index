#!/bin/bash
# `docker compose run --rm tsi-dbt` 의 기본 실행 스크립트.
# dbt run으로 로컬 .duckdb 파일(Silver/Gold)을 갱신한 뒤, Streamlit Cloud가
# 내려받을 수 있도록 같은 파일을 S3에 스냅샷으로 올린다.
set -e
dbt run
python publish_to_s3.py

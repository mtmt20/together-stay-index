-- Together-Stay Index - Snowflake 최초 1회 설정 스크립트
-- =====================================================
-- DAG가 매일 도는 COPY INTO 태스크와 달리, 이 스크립트는 "딱 한 번" 수동으로
-- 실행하는 DDL이다. Airflow 태스크로 넣지 않은 이유: 웨어하우스는 resume
-- 1회당 최소 60초가 과금되는데, DDL은 자주 바뀌지 않으므로 매일 도는
-- COPY INTO 태스크에 섞어 넣을 필요가 없다 (불필요한 재실행/재검토로
-- 웨어하우스를 깨우지 않기 위함).
--
-- 이름 규칙: 기존에 이미 돌고 있는 다른 프로젝트(위스키 파이프라인 등)의
-- Snowflake 객체와 절대 겹치지 않도록 DB/스키마/스테이지/테이블명 앞에
-- 전부 TOGETHER_STAY 계열 고유 이름을 사용한다.

CREATE DATABASE IF NOT EXISTS TOGETHER_STAY_DB;
CREATE SCHEMA IF NOT EXISTS TOGETHER_STAY_DB.RAW;

-- (2026-09-17 실제로는 STORAGE_INTEGRATION 대신 아래처럼 스테이지에
--  CREDENTIALS를 직접 넣는 방식으로 만들었다 - IAM Role 신뢰관계 설정
--  없이 바로 되고, 이미 S3 업로드용으로 발급해둔 액세스키를 그대로
--  재사용하면 되어서 더 간단하다. <..> 부분은 .env의 값으로 채울 것.)
CREATE STAGE IF NOT EXISTS TOGETHER_STAY_DB.RAW.NAVER_S3_STAGE
    URL = 's3://<AWS_S3_BUCKET>/bronze/together-stay-index/naver_raw/'
    CREDENTIALS = (AWS_KEY_ID='<AWS_ACCESS_KEY_ID>' AWS_SECRET_KEY='<AWS_SECRET_ACCESS_KEY>')
    FILE_FORMAT = (TYPE = PARQUET);

-- Raw 테이블. Parquet 컬럼을 VARIANT 하나로 받아서 실제 정형화는 dbt의
-- Bronze -> Silver 단계에서 처리한다 (Raw 레이어는 있는 그대로 적재만).
CREATE TABLE IF NOT EXISTS TOGETHER_STAY_DB.RAW.NAVER_POSTS_RAW (
    raw_data       VARIANT,
    source_file    STRING,
    loaded_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 맛집/카페 크롤러(food_cafe_crawler.py)용 스테이지/테이블. 숙소용과
-- 구조는 같지만 S3 경로/테이블을 분리해서 서로 안 섞이게 한다.
CREATE STAGE IF NOT EXISTS TOGETHER_STAY_DB.RAW.FOOD_CAFE_S3_STAGE
    URL = 's3://<AWS_S3_BUCKET>/bronze/together-stay-index/food_cafe_raw/'
    CREDENTIALS = (AWS_KEY_ID='<AWS_ACCESS_KEY_ID>' AWS_SECRET_KEY='<AWS_SECRET_ACCESS_KEY>')
    FILE_FORMAT = (TYPE = PARQUET);

CREATE TABLE IF NOT EXISTS TOGETHER_STAY_DB.RAW.FOOD_CAFE_POSTS_RAW (
    raw_data       VARIANT,
    source_file    STRING,
    loaded_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

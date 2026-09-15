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

-- S3 Bronze 레이어와 연동할 외부 스테이지.
-- STORAGE_INTEGRATION을 쓰는 것이 액세스키 하드코딩보다 안전하다.
-- (사전에 `CREATE STORAGE INTEGRATION together_stay_s3_int ...` 로
--  이 프로젝트 전용 S3 버킷만 바라보는 통합 객체를 별도로 만들어 둘 것.
--  기존 프로젝트의 storage integration을 재사용하지 말 것 - 버킷 정책이
--  섞이면 권한 반경이 넓어져 위험하다.)
CREATE STAGE IF NOT EXISTS TOGETHER_STAY_DB.RAW.NAVER_S3_STAGE
    URL = 's3://<TOGETHER-STAY-INDEX 전용 버킷명>/bronze/together-stay-index/naver_raw/'
    STORAGE_INTEGRATION = together_stay_s3_int
    FILE_FORMAT = (TYPE = PARQUET);

-- Raw 테이블. Parquet 컬럼을 VARIANT 하나로 받아서 실제 정형화는 dbt의
-- Bronze -> Silver 단계에서 처리한다 (Raw 레이어는 있는 그대로 적재만).
CREATE TABLE IF NOT EXISTS TOGETHER_STAY_DB.RAW.NAVER_POSTS_RAW (
    raw_data       VARIANT,
    source_file    STRING,
    loaded_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Together-Stay Index - 일일 COPY INTO 템플릿
-- =====================================================
-- Airflow DAG(together_stay_dag.py)의 copy_s3_to_snowflake_raw 태스크가
-- 이 SQL을 그대로 실행한다. Jinja 템플릿 변수 {{ ds }} 는 실행일자를
-- 나타내며, 그날 S3에 적재된 dt=YYYY-MM-DD/ 파티션만 골라서 COPY한다.
--
-- 매일 그 날짜 파티션만 지정해서 COPY하는 이유: 파티션을 안 걸면 스테이지
-- 전체를 스캔/재적재하려는 시도를 할 수 있어 웨어하우스 가동 시간이
-- 쓸데없이 늘어난다. COPY INTO는 기본적으로 이미 적재된 파일은 다시
-- 적재하지 않지만(load history 기준), 그래도 스캔 범위를 좁혀두는 것이
-- 트라이얼/저비용 환경에서는 안전하다.

COPY INTO TOGETHER_STAY_DB.RAW.NAVER_POSTS_RAW (raw_data, source_file)
FROM (
    SELECT
        $1 AS raw_data,
        METADATA$FILENAME AS source_file
    FROM @TOGETHER_STAY_DB.RAW.NAVER_S3_STAGE/dt={{ ds }}/
)
FILE_FORMAT = (TYPE = PARQUET)
ON_ERROR = 'CONTINUE';

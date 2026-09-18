-- Together-Stay Index - 맛집/카페 일일 COPY INTO 템플릿
-- =====================================================
-- copy_into_raw.sql과 완전히 같은 이유/구조. 대상 테이블/스테이지만 다르다.

COPY INTO TOGETHER_STAY_DB.RAW.FOOD_CAFE_POSTS_RAW (raw_data, source_file)
FROM (
    SELECT
        $1 AS raw_data,
        METADATA$FILENAME AS source_file
    FROM @TOGETHER_STAY_DB.RAW.FOOD_CAFE_S3_STAGE/dt={{ ds }}/
)
FILE_FORMAT = (TYPE = PARQUET)
ON_ERROR = 'CONTINUE';

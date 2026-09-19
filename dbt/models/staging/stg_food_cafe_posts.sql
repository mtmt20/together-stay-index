-- Silver: 맛집/카페 S3 Bronze parquet를 DuckDB httpfs로 직접 읽는다
-- (stg_naver_posts와 동일 패턴 - Snowflake RAW 적재 단계 없음).
select
    category,      -- "food" 또는 "cafe"
    region,
    keyword,
    source,        -- "blog" 또는 "cafearticle"
    title,
    link,
    description,
    full_text,
    try_cast(crawled_at as timestamptz)::timestamp as crawled_at,
    mentions_revisit,
    mentions_kids_friendly,
    mentions_nature,
    capacity_hint,
    price_hint
from read_parquet(
    's3://together-stay-index-bronze/bronze/together-stay-index/food_cafe_raw/dt=*/*.parquet',
    union_by_name = true
)
qualify row_number() over (
    partition by link
    order by crawled_at desc
) = 1

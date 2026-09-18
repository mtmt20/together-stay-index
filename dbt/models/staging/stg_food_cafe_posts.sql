-- Silver: 맛집/카페 RAW의 VARIANT를 정형 컬럼으로 펼치고, 같은 글(link)이
-- 여러 번 적재됐으면 최신 적재분만 남긴다 (stg_naver_posts와 동일 패턴).

select
    raw_data:category::string             as category,      -- "food" 또는 "cafe"
    raw_data:region::string                as region,
    raw_data:keyword::string               as keyword,
    raw_data:source::string                as source,
    raw_data:title::string                 as title,
    raw_data:link::string                  as link,
    raw_data:description::string           as description,
    raw_data:full_text::string             as full_text,
    raw_data:crawled_at::timestamp_ntz     as crawled_at,
    raw_data:mentions_revisit::boolean     as mentions_revisit,
    raw_data:mentions_kids_friendly::boolean as mentions_kids_friendly,
    raw_data:mentions_nature::boolean      as mentions_nature,
    raw_data:capacity_hint::number         as capacity_hint,
    raw_data:price_hint::number            as price_hint,
    source_file,
    loaded_at
from {{ source('raw', 'food_cafe_posts_raw') }}
qualify row_number() over (
    partition by link
    order by loaded_at desc
) = 1

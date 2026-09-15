-- Silver: RAW의 VARIANT 컬럼을 정형 컬럼으로 펼치고, 같은 글(link)이
-- 여러 번 크롤링돼 중복 적재됐을 경우 가장 최근 적재분만 남긴다.
-- (COPY INTO는 파일 단위 중복은 막아주지만, 크롤러가 같은 글을 다른 날
--  다시 수집해오는 경우까지는 막지 못하므로 여기서 dedupe한다.)

select
    raw_data:keyword::string          as keyword,
    raw_data:source::string           as source,
    raw_data:title::string            as title,
    raw_data:link::string             as link,
    raw_data:description::string      as description,
    raw_data:post_date::string        as post_date,
    raw_data:full_text::string        as full_text,
    raw_data:crawled_at::timestamp_ntz as crawled_at,
    source_file,
    loaded_at
from {{ source('raw', 'naver_posts_raw') }}
qualify row_number() over (
    partition by link
    order by loaded_at desc
) = 1

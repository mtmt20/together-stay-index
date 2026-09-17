-- Silver: RAW의 VARIANT 컬럼을 정형 컬럼으로 펼치고, 같은 글(link)이
-- 여러 번 크롤링돼 중복 적재됐을 경우 가장 최근 적재분만 남긴다.
-- (COPY INTO는 파일 단위 중복은 막아주지만, 크롤러가 같은 글을 다른 날
--  다시 수집해오는 경우까지는 막지 못하므로 여기서 dedupe한다.)
--
-- 2026-09-18: "화장실 2개" 같은 키워드는 여행/숙소뿐 아니라 인테리어·
-- 리모델링 카페 글에도 흔히 나온다는 게 확인됨 (예: "화장실 2개 리모델링
-- 비용"). 크롤러(dags/crawler/naver_crawler.py의 ACCOMMODATION_CONTEXT_TERMS)
-- 쪽에 이미 필터를 넣었지만, RAW에는 그 필터가 생기기 전에 적재된 옛날
-- 데이터도 남아있을 수 있어 여기서도 같은 기준으로 한 번 더 거른다
-- (RAW를 지우지 않고도 Gold에는 안 보이게 하려는 목적 - 두 목록은
-- 같이 유지할 것).
with staged as (
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
)

select *
from staged
where title || description ilike any (
    '%숙소%', '%펜션%', '%풀빌라%', '%게스트하우스%', '%한옥%', '%캠핑%',
    '%글램핑%', '%리조트%', '%여행%', '%숙박%', '%스테이%', '%카라반%'
)

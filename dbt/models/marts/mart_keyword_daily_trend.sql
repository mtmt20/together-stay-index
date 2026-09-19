-- Gold: 키워드별/일자별 언급량 추이. Streamlit 대시보드의 트렌드 차트가
-- 이 테이블 하나만 읽으면 되도록 미리 집계해둔다 (대시보드가 매번 RAW를
-- 스캔하지 않게 해서 Snowflake 웨어하우스 가동 시간을 줄이는 목적도 있음).

select
    keyword,
    date_trunc('day', crawled_at)::date as crawled_date,
    count(*)                                              as mention_count,
    count(distinct link)                                  as unique_post_count,
    sum(case when full_text is not null then 1 else 0 end) as with_full_text_count
from {{ ref('stg_naver_posts') }}
group by 1, 2
order by 1, 2

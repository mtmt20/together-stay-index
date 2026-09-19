-- Silver: S3 Bronze의 parquet 파일을 DuckDB httpfs로 직접 읽는다(read_parquet).
-- Snowflake 시절엔 COPY INTO로 RAW 테이블(VARIANT)에 옮겨 적재한 뒤 그걸
-- 펼쳤지만, DuckDB는 parquet를 그 자리에서 바로 정형 컬럼으로 읽을 수 있어
-- 그 적재 단계가 통째로 사라졌다. 같은 글(link)이 여러 번 크롤링돼 중복
-- 적재됐을 경우 가장 최근 크롤링분만 남긴다.
--
-- 2026-09-18: "화장실 2개" 같은 키워드는 여행/숙소뿐 아니라 인테리어·
-- 리모델링 카페 글에도 흔히 나온다는 게 확인됨 (예: "화장실 2개 리모델링
-- 비용"). 크롤러(dags/crawler/naver_crawler.py의 ACCOMMODATION_CONTEXT_TERMS)
-- 쪽에 이미 필터를 넣었지만, 예전에 적재된 옛날 parquet 파일에는 그
-- 필터가 없을 수 있어 여기서도 같은 기준으로 한 번 더 거른다.
with staged as (
    select
        keyword,
        source,
        title,
        link,
        description,
        post_date,
        full_text,
        try_cast(crawled_at as timestamptz)::timestamp as crawled_at
    from read_parquet(
        's3://together-stay-index-bronze/bronze/together-stay-index/naver_raw/dt=*/*.parquet',
        union_by_name = true
    )
    qualify row_number() over (
        partition by link
        order by crawled_at desc
    ) = 1
)

select *
from staged
where (title || description) ilike '%숙소%'
   or (title || description) ilike '%펜션%'
   or (title || description) ilike '%풀빌라%'
   or (title || description) ilike '%게스트하우스%'
   or (title || description) ilike '%한옥%'
   or (title || description) ilike '%캠핑%'
   or (title || description) ilike '%글램핑%'
   or (title || description) ilike '%리조트%'
   or (title || description) ilike '%여행%'
   or (title || description) ilike '%숙박%'
   or (title || description) ilike '%스테이%'
   or (title || description) ilike '%카라반%'

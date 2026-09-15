-- Gold: 본문까지 확보된 글만 걸러낸 "숙소 후보 언급 글" 리스트.
-- 실제 숙소명/지역 추출은 NLP가 필요해 대회 마감 전 뼈대 범위를 벗어나므로,
-- 우선 사람이 Streamlit에서 눈으로 훑어보며 큐레이션할 수 있도록 본문
-- 키워드 매칭 신호(화장실/마당 언급 여부)만 간단히 플래그로 얹어둔다.
-- 이 플래그는 이후 Silver 단계에 정규식/사전 매칭 로직으로 고도화하면 된다.

select
    keyword,
    source,
    title,
    link,
    description,
    full_text,
    crawled_at,
    full_text ilike '%화장실%' as mentions_bathroom,
    full_text ilike '%마당%'   as mentions_yard,
    full_text ilike '%독채%'   as mentions_whole_house
from {{ ref('stg_naver_posts') }}
where full_text is not null

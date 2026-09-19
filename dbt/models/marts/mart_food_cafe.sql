-- Gold: 지역별 "갈만한 맛집/카페" 큐레이션 리스트.
-- 사용자 요청(2026-09-18): "맛집도 재방문 높은순으로 아동친화요소 넣고 /
-- 카페도 자연 아동친화 노키즈존 피해서 넣고, 인원/가격/컨셉별 정리".
-- 노키즈존 제외는 이미 Silver 이전(크롤러)에서 처리됨 - 여기서는 정렬용
-- 우선순위 점수와 "인원/가격/컨셉" 표시를 위한 컬럼만 정리한다.

select
    category,
    region,
    title,
    link,
    source,
    crawled_at,
    mentions_revisit,
    mentions_kids_friendly,
    mentions_nature,
    capacity_hint,
    price_hint,
    -- 화면에서 바로 보여줄 컨셉 태그 (여러 개 동시 해당 가능).
    -- DuckDB에는 array_construct_compact/iff가 없어 list_filter로
    -- null을 걸러내는 방식으로 대체.
    list_filter(
        [
            case when mentions_revisit then '재방문많음' end,
            case when mentions_kids_friendly then '아동친화' end,
            case when mentions_nature then '자연/뷰' end
        ],
        x -> x is not null
    ) as concept_tags,
    -- 기본 정렬용 점수: 맛집은 재방문 우선, 카페는 자연+아동친화 우선
    case
        when category = 'food' then (mentions_revisit::int * 2) + mentions_kids_friendly::int
        when category = 'cafe' then mentions_nature::int + mentions_kids_friendly::int
        else 0
    end as recommend_score
from {{ ref('stg_food_cafe_posts') }}
order by region, category, recommend_score desc

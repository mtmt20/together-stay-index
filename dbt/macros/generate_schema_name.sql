{#
    dbt 기본 동작은 프로필의 기본 스키마(SILVER)와 모델별 +schema 설정(silver/gold)을
    "SILVER_silver", "SILVER_gold"처럼 합쳐버린다. 이러면 Streamlit 앱/README에서
    말하는 "GOLD 스키마"가 실제로는 존재하지 않게 된다. 이 프로젝트는 스키마
    이름을 모델 config에서 명시적으로 정하고 있으므로, 그 이름을 그대로 쓰도록
    dbt 기본 매크로를 덮어쓴다 (dbt 공식 문서가 권장하는 표준 커스터마이징 방식).
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}

{% macro surrogate_key(bus_key, sur_key) -%}
    case when {{ bus_key }} is not null then source_system_cd || '_' || row_id else null end as {{ sur_key }}
{%- endmacro %}

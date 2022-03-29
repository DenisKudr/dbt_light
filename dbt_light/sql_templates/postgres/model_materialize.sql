{% if model_exist %}
    {% if not is_incremental %}
        TRUNCATE TABLE {{ target_schema }}.{{ model }};
    {% endif %}
    INSERT INTO {{ target_schema }}.{{ model }} ({{ model_fields | join(', ') }})
    SELECT {{ model_fields | join(', ') }} FROM {{ model }};
{% else %}
    CREATE SCHEMA IF NOT EXISTS {{ target_schema }};
    {% if materialization == 'view' %}
        DROP VIEW IF EXISTS {{ target_schema }}.{{ model }} CASCADE;
        CREATE VIEW {{ target_schema }}.{{ model }} AS
        {{ model_sql }}
    {% else %}
        DROP TABLE IF EXISTS {{ target_schema }}.{{ model }} CASCADE;
        CREATE TABLE {{ target_schema }}.{{ model }} AS
        SELECT {{ model_fields | join(', ') }} FROM {{ model }};
    {% endif %}
{% endif %}

{% if is_incremental and incr_key and not model_exist %}
    alter table {{ target_schema }}.{{ model }} add column {{ incr_key }} serial;
{% endif %}
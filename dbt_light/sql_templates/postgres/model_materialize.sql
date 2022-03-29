{% if is_incremental and model_exist %}
    INSERT INTO {{ target_schema }}.{{ model }} ({{ model_fields | join(', ') }})
{% else %}
    {% if materialization == 'temp_table' %}
        CREATE TEMPORARY TABLE {{ model }} AS
    {% elif materialization == 'view' %}
        CREATE SCHEMA IF NOT EXISTS {{ target_schema }};
        DROP VIEW IF EXISTS {{ target_schema }}.{{ model }} CASCADE;
        CREATE VIEW {{ target_schema }}.{{ model }} AS
    {% else %}
        {% if not model_exist %}
            CREATE SCHEMA IF NOT EXISTS {{ target_schema }};
            DROP TABLE IF EXISTS {{ target_schema }}.{{ model }} CASCADE;
            CREATE TABLE {{ target_schema }}.{{ model }} AS
        {% else %}
            TRUNCATE TABLE {{ target_schema }}.{{ model }};
            INSERT INTO {{ target_schema }}.{{ model }} ({{ model_fields | join(', ') }})
        {% endif %}
    {% endif %}
{% endif %}
{{ model_sql }};

{% if is_incremental and incr_key and not model_exist %}
    alter table {{ target_schema }}.{{ model }} add column {{ incr_key }} serial;
{% endif %}
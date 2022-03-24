{% if is_incremental and model_exist %}
    INSERT INTO {{ model_schema }}.{{ model_name }} ({{ model_fields | join(', ') }})
{% else %}
    {% if materialization == 'temp_table' %}
        CREATE TEMPORARY TABLE {{ model_name }} AS
    {% elif materialization == 'view' %}
        CREATE SCHEMA IF NOT EXISTS {{ model_schema }};
        DROP VIEW IF EXISTS {{ model_schema }}.{{ model_name }};
        CREATE VIEW {{ model_schema }}.{{ model_name }} AS
    {% else %}
        CREATE SCHEMA IF NOT EXISTS {{ model_schema }};
        DROP TABLE IF EXISTS {{ model_schema }}.{{ model_name }};
        CREATE TABLE {{ model_schema }}.{{ model_name }} AS
    {% endif %}
{% endif %}
{{ model }};

{% if is_incremental and seq_key and not model_exist %}
    alter table {{ model_schema }}.{{ model_name }} add column {{ seq_key }} serial;
{% endif %}
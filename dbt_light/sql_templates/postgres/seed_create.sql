{% if seed_exist %}
    TRUNCATE TABLE {{ target_schema }}.{{ seed }};
{% else %}
    CREATE SCHEMA IF NOT EXISTS {{ target_schema }};
    DROP TABLE IF EXISTS {{ target_schema }}.{{ seed }} CASCADE;
    CREATE TABLE {{ target_schema }}.{{ seed }} (
        {% for field in seed_fields %}
            {{ field[0] }} {{ field[1] }}
            {{ ", " if not loop.last }}
        {% endfor %}
    );
{% endif %}
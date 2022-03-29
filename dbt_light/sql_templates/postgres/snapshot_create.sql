CREATE SCHEMA IF NOT EXISTS {{ target_schema }};
DROP TABLE IF EXISTS {{ target_schema }}.{{ snapshot }} CASCADE;
CREATE TABLE {{ target_schema }}.{{ snapshot }} AS
SELECT
    {% for field  in key_fields + all_data_fields %}
        {{ field }},
    {% endfor %}
    now()::timestamp  as {{ processed_field }},
    {% if strategy == "check" and (data_fields or hash_diff_field_exist) %}
        md5(ROW()::TEXT)::uuid as {{ hash_diff_field }},
    {% endif %}
    now()::timestamp  as {{ start_field }},
    now()::timestamp  as {{ end_field }}
FROM
    {% if delta_table == "temp_delta_table" %}
        {{ delta_table }} dff
    {% else %}
        {{ delta_schema}}.{{ delta_table }} dff
    {% endif %}
WHERE 1=2
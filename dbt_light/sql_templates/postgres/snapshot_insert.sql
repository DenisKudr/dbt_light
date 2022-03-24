INSERT INTO {{ target_schema }}.{{ snapshot }}
(
    {% for field  in key_fields + all_data_fields + new_fields %}
        {{ field }},
    {% endfor %}
    {{ processed_field }},
    {% if strategy == "check" and (data_fields or hash_diff_field_exist) %}
        {{ hash_diff_field }},
    {% endif %}
    {{ start_field }},
    {{ end_field }}
    )
SELECT
    {% for field  in key_fields + all_data_fields + new_fields  %}
        {{ field }},
    {% endfor %}
    {{ processed_field }},
    {% if strategy == "check" and (data_fields or hash_diff_field_exist) %}
        {{ hash_diff_field }},
    {% endif %}
    {% if updated_at_field %}
        {{ updated_at_field }},
    {% else %}
        {{ processed_field }},
    {% endif %}
    {% if max_dttm %}
        '{{ max_dttm }}'::timestamp
    {% else %}
        null
    {% endif %}
FROM
    {% if delta_table == "temp_delta_table" %}
        {{ delta_table }} dff
    {% else %}
        {{ delta_schema}}.{{ delta_table }} dff
    {% endif %}
WHERE
    dff.diff_type not in ('DU', 'DO')
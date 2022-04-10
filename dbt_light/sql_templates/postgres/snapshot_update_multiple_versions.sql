UPDATE {{ target_schema }}.{{ snapshot }} as dds
SET
{{ end_field }} = dff.closing_date
FROM
    (SELECT {{ key_fields | join(', ') }}, {{ updated_at_field }}, closing_date
    FROM (SELECT {{ key_fields | join(', ') }}, {{ updated_at_field }},
                 row_number() over (partition by {{ key_fields | join(', ') }} order by {{ updated_at_field }} desc) as row_num,
                 lag({{ updated_at_field }}) over (partition by {{ key_fields | join(', ') }} order by {{ updated_at_field }} desc) as closing_date
          FROM
                {% if delta_table == "temp_delta_table" %}
                    {{ delta_table }} dff
                {% else %}
                    {{ delta_schema}}.{{ delta_table }} dff
                {% endif %}) a
    WHERE row_num > 1) dff
WHERE
    dds.{{ end_field }}
        {% if max_dttm %}
            = '{{ max_dttm }}'::timestamp
        {% else %}
            is null
        {% endif %} and
    dds.{{ start_field }} = dff.{{ updated_at_field }} and
    {% for field in key_fields %}
        dds.{{ field }} = dff.{{ field }}
    {{ "and " if not loop.last }}
    {% endfor %}
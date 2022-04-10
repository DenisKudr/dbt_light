UPDATE {{ target_schema }}.{{ snapshot }} as dds
SET
    {{ processed_field }} = case
        when dff.diff_type = 'DU' then dff.{{ processed_field }}
        else dds.{{ processed_field }}
    end,
    {% if strategy == "check" and (data_fields or hash_diff_field_exist) %}
        {{ hash_diff_field }} = case
            when dff.diff_type = 'DU' then dff.{{ hash_diff_field }}
            else dds.{{ hash_diff_field }}
        end,
    {% endif %}
    {% for data_field in all_data_fields %}
        {{ data_field }} = case
            when dff.diff_type = 'DU' then dff.{{ data_field }}
            else dds.{{ data_field }}
            end,
    {% endfor %}
    {% for new_field in new_fields %}
        {{ new_field }} = case
            when dff.diff_type = 'DU' then dff.{{ new_field }}
            else dds.{{ new_field }}
            end,
    {% endfor %}
    {{ end_field }} = case
        when dff.diff_type = 'DU' then dds.{{ end_field }}
        when dff.diff_type = 'DO' then
        {% if deleted_flg %}
            dff.{{ updated_at_field }}
        {% else %}
            dff.{{ processed_field }}
        {% endif %}
        else
        {% if updated_at_field %}
            dff.{{ updated_at_field }}
        {% else %}
            dff.{{ processed_field }}
        {% endif %}
    end
FROM
    {% if delta_table == "temp_delta_table" %}
        {{ delta_table }} dff
    {% else %}
        {{ delta_schema}}.{{ delta_table }} dff
    {% endif %}
WHERE
    dds.{{ end_field }}
        {% if max_dttm %}
            = '{{ max_dttm }}'::timestamp
        {% else %}
            is null
        {% endif %} and
    {% for field in key_fields %}
        dds.{{ field }} = dff.{{ field }}
    {{ "and " if not loop.last }}
    {% endfor %}
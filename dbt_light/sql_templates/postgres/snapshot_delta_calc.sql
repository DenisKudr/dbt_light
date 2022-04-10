{% if delta_table == "temp_delta_table" %}
    CREATE TEMPORARY TABLE {{ delta_table }} as
{% else %}
    {% if init_load %}
        CREATE SCHEMA IF NOT EXISTS {{ delta_schema }};
        DROP TABLE if exists {{ delta_schema }}.{{ delta_table }} CASCADE;
        CREATE TABLE {{ delta_schema }}.{{ delta_table }} AS
    {% else %}
        TRUNCATE TABLE {{ delta_schema }}.{{ delta_table }};
        INSERT INTO {{ delta_schema }}.{{ delta_table }} (
            {% for field  in key_fields + all_data_fields + new_fields %}
                {{ field }},
            {% endfor %}
            {% if updated_at_field %}
                {{ updated_at_field }},
            {% endif %}
            {{ processed_field }},
            {% if strategy == "check" and (hash_diff_field_exist or data_fields) %}
                {{ hash_diff_field }},
            {% endif %}
            diff_type
        )
    {% endif %}
{% endif %}
{% if init_load %}
    SELECT
        {% for field  in key_fields + all_data_fields %}
            src.{{ field }},
        {% endfor %}
        {% if updated_at_field %}
            src.{{ updated_at_field }},
        {% endif %}
        {% if processed_field_exist %}
            src.{{ processed_field }},
        {% else %}
            now()::timestamp as {{ processed_field }},
        {% endif %}
        {% if strategy == "check" %}
            {% if hash_diff_field_exist %}
                src.{{ hash_diff_field }},
            {% else %}
                {% if data_fields %}
                    md5(row(
                    {% for data_field in data_fields %}
                        src.{{ data_field }}
                        {{ ", " if not loop.last }}
                    {% endfor %}
                            )::TEXT)::uuid as {{ hash_diff_field }},
                {% endif %}
            {% endif %}
        {% endif %}
        {% if deleted_flg %}
            case when {{ deleted_flg }} = '{{ deleted_flg_val }}' then 'DO'
            else 'NO' end as diff_type
        {% else %}
           'NO' as diff_type
        {% endif %}

    FROM
            {{ source_schema }}.{{ input_table }} as src
{% else %}
    SELECT
        {% for field  in key_fields + all_data_fields %}
            case when src.{{ key_fields | first }} is not null then src.{{ field }}
                else dds.{{ field }} end as {{ field }},
        {% endfor %}
        {% for field in new_fields %}
            src.{{ field }},
        {% endfor %}
        {% if updated_at_field %}
            src.{{ updated_at_field }},
        {% endif %}
        {% if processed_field_exist %}
            src.{{ processed_field }},
        {% else %}
            now()::timestamp as {{ processed_field }},
        {% endif %}
        {% if strategy == "check" %}
            {% if hash_diff_field_exist %}
                src.{{ hash_diff_field }},
            {% else %}
                {% if data_fields %}
                    case when src.{{ key_fields | first }} is not null then md5(row(
                    {% for data_field in data_fields %}
                        src.{{ data_field }}
                        {{ ", " if not loop.last }}
                    {% endfor %}
                            )::TEXT)::uuid
                            else dds.{{ hash_diff_field }} end as {{ hash_diff_field }},
                {% endif %}
            {% endif %}
        {% endif %}
        case
            when src.{{ key_fields | first }} is null
            {% if deleted_flg %}
                or {{ deleted_flg }} = '{{ deleted_flg_val }}'
            {% endif %}
            then 'DO'
            when dds.{{ key_fields | first }} is null then 'NO'
            when dds.{{ start_field }} =
                {% if updated_at_field %}
                    src.{{ updated_at_field }}
                {% elif processed_field_exist %}
                    src.{{ processed_field }}
                {% else %}
                    now()::timestamp
                {% endif %}
            then 'DU'
            else 'NV'
        end as diff_type
    FROM
        {{ source_schema }}.{{ input_table }} as src full join
            ( SELECT
                dds.*
                from {{ target_schema }}.{{ snapshot }} as dds
            WHERE
                dds.{{ end_field }}
                 {% if max_dttm %}
                    = '{{ max_dttm }}'::timestamp
                 {% else %}
                    is null
                 {% endif %}
            ) as dds
        on
        {% for field  in key_fields %}
            src.{{ field }} = dds.{{ field }}
            {{ " and " if not loop.last }}
        {% endfor %}
    WHERE
        dds.{{ key_fields | first }} is null
        {% if invalidate_hard_deletes %}
            or src.{{ key_fields | first}} is null
        {% endif %}
        {% if strategy == "check" and (data_fields or hash_diff_field_exist) %}
            or (src.{{ key_fields | first }} is not null and dds.{{ key_fields | first }} is not null and
            {% if hash_diff_field_exist %}
                src.{{ hash_diff_field }} <> dds.{{ hash_diff_field }})
            {% else %}
                {% if data_fields %}
                    md5(row(
                    {% for data_field in data_fields %}
                        src.{{ data_field }}
                        {{ ", " if not loop.last }}
                    {% endfor %}
                            )::TEXT)::uuid <> dds.{{ hash_diff_field }})
                {% endif %}
            {% endif %}
            {% if updated_at_field %}
                and dds.{{ start_field }} <= src.{{ updated_at_field }}
            {% endif %}
        {% endif %}

        {% if strategy == "timestamp" %}
            or (src.{{ key_fields | first }} is not null and dds.{{ key_fields | first }} is not null
            and src.{{ updated_at_field }} > dds.{{ start_field }})
        {% endif %}
{% endif %}
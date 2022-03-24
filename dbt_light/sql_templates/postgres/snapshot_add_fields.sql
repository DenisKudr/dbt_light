{% for field in new_fields_with_datatypes %}
        ALTER TABLE {{ target_schema }}.{{ snapshot }} ADD {{ field[0] }} {{ field[1] }}{% if field[2] %}({{ field[2] }}){% endif %};
{% endfor %}
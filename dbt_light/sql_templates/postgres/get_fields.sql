SELECT upper(column_name)
{% if include_data_types %}
    , data_type, character_maximum_length
{% endif %}
FROM information_schema.columns
WHERE  upper(table_schema) = upper('{{ schema }}')
and upper(table_name) = upper('{{ model }}')
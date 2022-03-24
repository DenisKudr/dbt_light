SELECT EXISTS(
    SELECT *
    FROM information_schema.tables
    WHERE
      table_schema = '{{ model_schema }}' AND
      table_name = '{{ model_name }}'
);
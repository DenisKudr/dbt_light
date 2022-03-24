SELECT
    -- New objects
    coalesce(sum(case when dff.diff_type = 'NO' then 1 else 0 end), 0) as num_no,
    -- New value
    coalesce(sum(case when dff.diff_type = 'NV' then 1 else 0 end), 0) as num_nv,
    -- Deleted objects
    coalesce(sum(case when dff.diff_type = 'DO' then 1 else 0 end), 0) as num_do,
    -- Direct update objects
    coalesce(sum(case when dff.diff_type = 'DU' then 1 else 0 end), 0) as num_du
FROM
    {% if delta_table == "temp_delta_table" %}
        {{ delta_table }} dff
    {% else %}
        {{ delta_schema}}.{{ delta_table }} dff
    {% endif %}

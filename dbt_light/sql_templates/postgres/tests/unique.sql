SELECT exists(SELECT count(*) FROM {{ model }} GROUP BY {{ column }}
{% if start_field %}
, {{ start_field }}
{% endif %}
HAVING count(*) > 1)
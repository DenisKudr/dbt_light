{% set count_codes = statement('select country_code from {{ seed }}') %}

select * from {{ seed }} where country_code in (
{% for cc in count_codes %}
	'{{ cc }}'
	{{ ', ' if not loop.last }}
{% endfor %}
)


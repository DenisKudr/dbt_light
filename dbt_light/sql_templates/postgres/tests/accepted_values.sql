SELECT exists(SELECT {{ column }} FROM {{ model }}
WHERE {{ column }} not in (
{% for val in values %}
    '{{ val }}'
    {{ ", " if not loop.last }}
{% endfor %})
)
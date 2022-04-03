SELECT exists(SELECT m.{{ column }} from {{ model }} m left join {{ schemas_context[to] }} t on m.{{ column }} = t.{{ field }}
WHERE t.{{ field }} is null)
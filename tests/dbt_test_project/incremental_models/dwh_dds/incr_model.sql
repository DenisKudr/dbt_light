select distinct customer_bk, row_id, source_system_cd, last_upd_dttm from {{ usual_model }}
{% if model_exist %}
	where customer_bk not in (select customer_bk from {{ this }})
{% endif %}
select * from {{ usual_model }}
{% if model_exist %}
	where customer_bk not in (select customer_bk from {{ this }})
{% endif %}

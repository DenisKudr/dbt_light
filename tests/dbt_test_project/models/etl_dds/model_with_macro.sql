select *, source_system_cd || '_' || row_id as customer_bk, 
{{ surrogate_key('phone_id', 'customer_phone_bk') }}
from etl_stg.model_in

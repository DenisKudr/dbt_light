select *, source_system_cd || '_' || row_id as customer_bk, 
case when phone_id is not null then source_system_cd || '_' || row_id else null end as customer_phone_bk 
from etl_stg.model_in

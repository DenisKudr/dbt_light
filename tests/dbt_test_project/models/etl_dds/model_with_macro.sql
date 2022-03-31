with cte_table as (
select *, source_system_cd || '_' || row_id as customer_bk, 
{{ surrogate_key('phone_id', 'customer_phone_bk') }}
from {{ test_source.model_in }} )
select * from cte_table

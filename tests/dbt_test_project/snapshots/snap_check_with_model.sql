select n.*, now()::timestamp as new_ts, 'Y' as new_flg from etl_stg.check_in n
where id_field <> 5
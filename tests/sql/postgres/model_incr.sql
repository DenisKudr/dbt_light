truncate table etl_stg.model_in;

INSERT INTO etl_stg.model_in(
	row_id, source_system_cd, first_name, last_upd_dttm, phone_id)
	VALUES ('4', 'ODP', 'Vadim', '2022-01-01 00:00:00'::timestamp, '4'),
			('5', 'ODP', 'John', '2022-01-02 00:00:00'::timestamp, '5');

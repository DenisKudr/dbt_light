truncate table etl_stg.model_in;

alter table etl_stg.model_in add column last_name varchar(20);

INSERT INTO etl_stg.model_in(
	row_id, source_system_cd, first_name, last_name, last_upd_dttm, phone_id)
	VALUES ('4', 'ODP', 'Vadim', 'Johnson', '2022-01-01 00:00:00'::timestamp, '4'),
			('5', 'ODP', 'John', 'Smith', '2022-01-02 00:00:00'::timestamp, '5');

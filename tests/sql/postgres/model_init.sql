DROP TABLE IF EXISTS etl_stg.model_in;

CREATE TABLE IF NOT EXISTS etl_stg.model_in
(
    row_id character varying(100) COLLATE pg_catalog."default",
    source_system_cd character(3) COLLATE pg_catalog."default",
    first_name character varying(30) COLLATE pg_catalog."default",
    last_upd_dttm timestamp without time zone,
    phone_id character varying(100) COLLATE pg_catalog."default");

INSERT INTO etl_stg.model_in(
	row_id, source_system_cd, first_name, last_upd_dttm, phone_id)
	VALUES ('1', 'ODP', 'Denis', '2022-01-01 00:00:00'::timestamp, '1'),
			('2', 'ODP', 'Maxim', '2022-01-02 00:00:00'::timestamp, '2'),
			('3', 'ODP', 'Anton', '2022-01-05 00:00:00'::timestamp, null);


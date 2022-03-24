DROP TABLE IF EXISTS etl_stg.siebel_s_contact;

CREATE TABLE IF NOT EXISTS etl_stg.siebel_s_contact
(
    row_id character varying(100) COLLATE pg_catalog."default",
    source_system_cd character(3) COLLATE pg_catalog."default",
    first_name character varying(30) COLLATE pg_catalog."default",
    last_upd_dttm timestamp without time zone,
    phone_id character varying(100) COLLATE pg_catalog."default"
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS etl_stg.siebel_s_contact
    OWNER to postgres;

GRANT ALL ON TABLE etl_stg.siebel_s_contact TO postgres;

GRANT ALL ON TABLE etl_stg.siebel_s_contact TO PUBLIC;

INSERT INTO etl_stg.siebel_s_contact(
	row_id, source_system_cd, first_name, last_upd_dttm, phone_id)
	VALUES ('1', 'ODP', 'Denis', '2022-01-01 00:00:00'::timestamp, '1'), 
			('2', 'ODP', 'Maxim', '2022-01-02 00:00:00'::timestamp, '2'), 
			('3', 'ODP', 'Anton', '2022-01-05 00:00:00'::timestamp, null);


DROP TABLE IF EXISTS etl_stg.siebel_s_phone;

CREATE TABLE IF NOT EXISTS etl_stg.siebel_s_phone
(
    row_id character varying(100) COLLATE pg_catalog."default",
    source_system_cd character(3) COLLATE pg_catalog."default",
    phone_num character varying(30) COLLATE pg_catalog."default",
    last_upd_dttm timestamp without time zone
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS etl_stg.siebel_s_phone
    OWNER to postgres;

GRANT ALL ON TABLE etl_stg.siebel_s_phone TO postgres;

GRANT ALL ON TABLE etl_stg.siebel_s_phone TO PUBLIC;

INSERT INTO etl_stg.siebel_s_contact(
	row_id, source_system_cd, first_name, last_upd_dttm, phone_id)
	VALUES ('1', 'ODP', '+7926221313', '2022-01-01 00:00:00'::timestamp), 
			('2', 'ODP', '+7818237321', '2022-01-02 00:00:00'::timestamp);



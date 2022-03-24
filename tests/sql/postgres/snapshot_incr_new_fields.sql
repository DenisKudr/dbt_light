DROP TABLE IF EXISTS etl_stg.check_in;

CREATE  table etl_stg.check_in
(
    pension_id integer NOT NULL,
    pension_schema character varying(50) COLLATE pg_catalog."default",
    new_field char(5),
    source_system_cd char(3),
    last_upd_dttm timestamp without time zone
);

ALTER TABLE IF EXISTS etl_stg.check_in
    OWNER to postgres;

INSERT INTO etl_stg.check_in (pension_id, pension_schema, new_field, source_system_cd, last_upd_dttm) VALUES (2, 'Схема 2 Changed', 'new', 'ODP',  '2022-01-08 00:00:00');
INSERT INTO etl_stg.check_in (pension_id, pension_schema, new_field, source_system_cd, last_upd_dttm) VALUES (5, 'Схема 5 New', 'new', 'ODP', '2022-01-20 00:00:00');
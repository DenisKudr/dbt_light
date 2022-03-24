DROP TABLE IF EXISTS etl_stg.check_in;

CREATE  table etl_stg.check_in
(
    pension_id integer NOT NULL,
    pension_schema character varying(50) COLLATE pg_catalog."default",
    source_system_cd char(3),
    last_upd_dttm timestamp without time zone
);

ALTER TABLE IF EXISTS etl_stg.check_in
    OWNER to postgres;


INSERT INTO etl_stg.check_in (pension_id, pension_schema, source_system_cd, last_upd_dttm) VALUES (1, 'Схема 1', 'ODP', '2022-01-01 00:00:00');
INSERT INTO etl_stg.check_in (pension_id, pension_schema, source_system_cd, last_upd_dttm) VALUES (2, 'Схема 2',  'ODP','2022-01-02 00:00:00');
INSERT INTO etl_stg.check_in (pension_id, pension_schema, source_system_cd, last_upd_dttm) VALUES (3, 'Схема 3', 'ODP','2022-01-03 00:00:00');
INSERT INTO etl_stg.check_in (pension_id, pension_schema, source_system_cd, last_upd_dttm) VALUES (4, 'Схема 4', 'ODP','2022-01-04 00:00:00');

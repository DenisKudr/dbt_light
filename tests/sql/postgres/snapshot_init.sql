DROP TABLE IF EXISTS etl_stg.check_in;

CREATE  table etl_stg.check_in
(
    id_field integer NOT NULL,
    changing_field character varying(50) COLLATE pg_catalog."default",
    ignored_field char(3),
    updated_at_field timestamp without time zone
);

INSERT INTO etl_stg.check_in (id_field, changing_field, ignored_field, updated_at_field) VALUES (1, 'Schema 1', 'ODP', '2022-01-01 00:00:00');
INSERT INTO etl_stg.check_in (id_field, changing_field, ignored_field, updated_at_field) VALUES (2, 'Schema 2',  'ODP','2022-01-02 00:00:00');
INSERT INTO etl_stg.check_in (id_field, changing_field, ignored_field, updated_at_field) VALUES (3, 'Schema 3', 'ODP','2022-01-03 00:00:00');
INSERT INTO etl_stg.check_in (id_field, changing_field, ignored_field, updated_at_field) VALUES (4, 'Schema 4', 'ODP','2022-01-04 00:00:00');

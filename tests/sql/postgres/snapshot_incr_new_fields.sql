DROP TABLE IF EXISTS etl_stg.check_in;

CREATE  table etl_stg.check_in
(
    id_field integer NOT NULL,
    changing_field character varying(50) COLLATE pg_catalog."default",
    new_field char(5),
    ignored_field char(3),
    updated_at_field timestamp without time zone
);

INSERT INTO etl_stg.check_in (id_field, changing_field, new_field, ignored_field, updated_at_field) VALUES (2, 'Schema 2 Changed', 'new', 'ODP',  '2022-01-08 00:00:00');
INSERT INTO etl_stg.check_in (id_field, changing_field, new_field, ignored_field, updated_at_field) VALUES (5, 'Schema 5 New', 'new', 'ODP', '2022-01-20 00:00:00');
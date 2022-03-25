DROP TABLE IF EXISTS etl_stg.check_in_no_data;

CREATE  table etl_stg.check_in_no_data
(
    id_field integer NOT NULL,
    updated_at_field timestamp without time zone
);

INSERT INTO etl_stg.check_in_no_data (id_field,  updated_at_field) VALUES (1,   '2022-01-01 00:00:00');
INSERT INTO etl_stg.check_in_no_data (id_field, updated_at_field) VALUES (2, '2022-01-07 00:00:00');

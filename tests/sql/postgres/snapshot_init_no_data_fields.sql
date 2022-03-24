DROP TABLE IF EXISTS etl_stg.check_in_no_data;

CREATE  table etl_stg.check_in_no_data
(
    pension_id integer NOT NULL,
    last_upd_dttm timestamp without time zone
);

ALTER TABLE IF EXISTS etl_stg.check_in_no_data
    OWNER to postgres;


INSERT INTO etl_stg.check_in_no_data (pension_id,  last_upd_dttm) VALUES (1,   '2022-01-01 00:00:00');
INSERT INTO etl_stg.check_in_no_data (pension_id, last_upd_dttm) VALUES (2, '2022-01-07 00:00:00');

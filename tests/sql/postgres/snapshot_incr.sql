truncate table etl_stg.check_in;

INSERT INTO etl_stg.check_in (pension_id, pension_schema, source_system_cd, last_upd_dttm) VALUES (2, 'Схема 2 Changed', 'ODP',  '2022-01-08 00:00:00');
INSERT INTO etl_stg.check_in (pension_id, pension_schema, source_system_cd, last_upd_dttm) VALUES (3, 'Схема 3 Direct Changed', 'ODP', '2022-01-03 00:00:00');
INSERT INTO etl_stg.check_in (pension_id, pension_schema, source_system_cd, last_upd_dttm) VALUES (4, 'Схема 4', 'OD', '2022-01-10 00:00:00');
INSERT INTO etl_stg.check_in (pension_id, pension_schema, source_system_cd, last_upd_dttm) VALUES (5, 'Схема 5 New', 'ODP', '2022-01-20 00:00:00');
truncate table etl_stg.check_in;

INSERT INTO etl_stg.check_in (id_field, changing_field, ignored_field, updated_at_field) VALUES (2, 'Schema 2 Changed', 'ODP',  '2022-01-08 00:00:00');
INSERT INTO etl_stg.check_in (id_field, changing_field, ignored_field, updated_at_field) VALUES (3, 'Schema 3 Direct Changed', 'ODP', '2022-01-03 00:00:00');
INSERT INTO etl_stg.check_in (id_field, changing_field, ignored_field, updated_at_field) VALUES (4, 'Schema 4', 'OD', '2022-01-10 00:00:00');
INSERT INTO etl_stg.check_in (id_field, changing_field, ignored_field, updated_at_field) VALUES (5, 'Schema 5 New', 'ODP', '2022-01-20 00:00:00');
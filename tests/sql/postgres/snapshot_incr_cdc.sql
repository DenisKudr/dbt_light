truncate table etl_stg.check_in_cdc;

INSERT INTO etl_stg.check_in_cdc (id_field, changing_field, ignored_field, updated_at_field, cdc_field) VALUES
(4, 'Schema 4 New1', 'ODP', '2022-01-21 00:00:00', 'Update'),
(4, 'Schema 4 New2', 'ODP', '2022-01-22 00:00:00', 'Update'),
(6, 'Schema 6', 'ODP', '2022-01-22 00:00:00', 'Insert'),
(6, 'Schema 6 New', 'ODP', '2022-01-23 00:00:00', 'Update'),
(7, 'Schema 7', 'ODP', '2022-01-25 00:00:00', 'Insert'),
(7, 'Schema 7', 'ODP', '2022-01-26 00:20:00', 'Delete'),
(7, 'Schema 7 Again!', 'ODP', '2022-01-27 00:00:00', 'Insert'),
(1, 'Schema 1', 'ODP', '2022-01-04 00:00:00', 'Delete'),
(2, 'Schema 2 New',  'ODP','2022-01-03 00:00:00', 'Update');
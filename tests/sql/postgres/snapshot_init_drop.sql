CREATE schema if not exists etl_stg;
DROP TABLE IF EXISTS dwh_dds.snap_check;
DROP TABLE IF EXISTS dwh_dds.snap_timestamp;
DROP TABLE IF EXISTS dwh_dds.snap_check_no_delta_table;
DROP TABLE IF EXISTS dwh_dds.snap_check_processed;
DROP TABLE IF EXISTS dwh_dds.snap_check_no_data;
DROP TABLE IF EXISTS dwh_dds.snap_check_new_fields;
DROP TABLE IF EXISTS dwh_dds.snap_check_with_model;
DROP TABLE IF EXISTS  dwh_dds.snap_timestamp_cdc;
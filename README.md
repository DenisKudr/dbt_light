Lightweight data build tool 
===============================================================================

**dbt-light** is a library for developing transformations inside data warehouse. It handles the T in ELT processes by turning select queries templated with [jinja2](https://github.com/pallets/jinja) into tables/views without the need to write and apply ddl as it will automatically handle objects creation and addition of new columns.  

The purpose of DWH is to store history, **dbt-light** does that by using highly configurable snapshots that implement SCD2. They can handle CDC full/delta loads, events and store relationships. 

Example
-------------------------------------------------------------------------------

**dbt-light** has a simple API that allows object creation by its name in project directory:

```python
>>> from dbt_light.model import Model
>>> from dbt_light.snapshot import Snapshot
>>> from dbt_light.seed import Seed

>>> Model('customer_data').materialize()
>>> Seed('customer_ref').materialize()
>>> Snapshot('customer_snapshot').materialize()
```

Installation
-------------------------------------------------------------------------------

Use `pip`:

    pip install dbt-light

Then execute the module to create new dbt project:

    python -m dbt-light 

This will create ~/.dbt-light/profiles.yaml if it doesn't exist already. This file contains all dbt projects with paths to their folders as well as connection parameters to target db (only postgres is supproted now). 

Example of *profiles.yaml* file:
```yaml
dbt-light_project:
  path: /home/user/dbt-light_project
  adapter: postgres
  dbname: postgres
  host: localhost
  pass: pass
  port: 5432
  user: postgres
```
This will also create template folder with dbt-light project in your current directory with the following structure:
```bash
├── dbt-light_project
│   ├── models
│   ├── incremental_models
│   ├── snapshots
│   ├── seeds
│   ├── macros
```

Configuration
-------------------------------------------------------------------------------

All configuration is done in yaml files separately for each type of object (models, snapshots, seeds, sources) in project directory. There's no need to create these files for objects you don't use. There are three ways to define configuration properties: for all objects at the top of yaml file, for objects which name follows specific pattern with *pattern_name* key and for specific object with *name* key. These methods can be combined, in this case more specific property will be chosen.

For example, if there's a *models.yaml* file in project's directory such as:

```yaml
materialization: table
models:
  - pattern_name: "vw_.*"
    materialization: view
  - name: vw_model
    materialization: table
```
All models will be created as tables unless they follow the pattern "vw_.\*" except for model with the name "vw_model".

Basic classes
-------------------------------------------------------------------------------

There are three main classes: `Model`, `Snapshot` and `Seed`. They can be created by passing object's name and if there are more than one projects defined in *profiles.yaml* you will also need to pass `dbt_project` param with the name of the project. All object types have `full_refresh` constructor parameter which is False by default. 

These classes have method `materialize` to load the object in database. By default, object (and its schema if not exists) gets created on the first run and insert (sometimes truncate before that) is performed on all consecutive runs. If `full_refresh` is passed as True than *DROP CASCADE* will be used which will also drop all objects that depend on it. Models with *view* materialization are dropped with cascade every time even if `full_refresh` is False.

`dbt-light` will handle adding new fields without loading objects with `full_refresh` set to True. So `full_refresh` can be used to recreate objects to delete fields that are no longer used, by default *null* will be inserted into them. Or it can be used to recreate objects that have state like snapshots and incremental models but in this case all the history will be lost. 

Jinja variables can be used to reference another object by its name. `dbt-light` will interpolate schema when rendering the object.

Models
-------------------------------------------------------------------------------

`Models` configuration is specified in *models.yaml* but some properties are implied. For example, target schema is defined by the subfolder name in models/incremental_models in which sql query resides. Whether model's incremental or not is defined by the parent folder name (models or incremental_models). By default, model will be created as table unless materialization property is defined as view in *models.yaml*. 

For incremental models there's a way to filter new rows added to it. Jinja variable `model_exist` can be used to determine whether model already exists and model itself can be referenced with jinja variable `this`. Incremental models can have sequence key which gets created if `incr_key` property is defined for the model which can be used to generate integer surrogate keys. However, it is much better to use hash keys.

For example:
```sql
select * from {{ some_model }}
{% if model_exist %}
	where customer_id not in (select customer_id from {{ this }})
{% endif %}
```

Snapshots
-------------------------------------------------------------------------------

`Snapshots` implement SCD2 (Slowly Changing Dimensions) to identify how row changes over time. If you're building snapshot based on another object you can set property `model` in *snapshots.yaml*, that way you don't need to create .sql file for that snapshot. Because of that unlike models target schema is defined in configuration file with the key `target_schema` rather than implied by the subfolder name. Unique key or keys should be defined with `key_fields` for each snapshot. `target_schema` and `key_fields` are two only required properties. But .sql file should exist in *snapshots/* or `model` should be defined otherwise `NoInputSpecifiedError` will be raised.

Method `materialize` can be used to load a snapshot as well as methods `delta_calc` and `delta_apply` if there's a need to calculate and apply changes in separate steps. Method `delta_calc` will create delta table containing all changes and `delta_apply` will load changes from that table. In order to use these methods `delta_schema` and `delta_table` should be defined for that model otherwise temporary table is used when calculating delta which will not exist when calling `delta_apply`. Delta table has some additional columns including *processed_dttm*, *hash_diff* (if there are data columns) and *diff_type* which indicate the type of change. There are four diff types:

 * NO - new object
 * NV - new value of existing object
 * DO - deleted object
 * DU - direct update (when source timestamp hasn't changed but hash sum's changed)

There are two ways of detecting changes: *check* (used by default) and *timestamp*. 

*Timestamp* will detect changes by comparing `updated_at_field` timestamp which indicates if a row has changed in source system. It gives better performance but should be used only if timestamp is reliable. If changes were made but `updated_at_field` didn't change, new version won't be created or if timestamp changed but no actual changes were made, new version will be created anyway. 

*Check* strategy detects changes by comparing hash sums of data columns. By default, all data columns are used but you can specify columns in `data_fields` or set `ignored_data_fields` to exclude some of them. Hash sum will be stored in column with the name *hash_diff* but you can change the name with `hash_diff_field`. `dbt-light` uses md5 to calculate hash sum but you can calculate it in advance using your own method and if `hash_diff_field` is in input table it will be used instead of calculating a new one. 

There can be no data columns at all. If you're modeling many-to-many relationship you may find it useful to create snapshot to see when relationship started and ended (when row was deleted from source). In this case `hash_diff_field` will not be created.

*Check* strategy can also be used with `updated_at_field`. In this case you avoid some problems with unreliable timestamp that occur when using *timestamp* strategy. New version will not be created if `updated_at_field` changed and data columns didn't. And if changes were made but `updated_at_field` didn't change row will be updated directly without creating new version (DU diff type in delta table).  

If `updated_at_field` is not specified then `processed_field` (*processed_dttm* by default) is used to create date intervals using current timestamp. If you want to use other timestamp, for example, timestamp when a row was extracted from source to a staging area than you can specify it in `processed_field` property and it will be used instead of current datetime. `updated_at_field` can also be used for this purpose but it'll be less clear as `updated_at_field` is used for source timestamps.

Rows that are absent in input table but exist in snapshot by default will get invalidated unless `invalidate_hard_deletes` is set to False. `processed_field` or `updated_at_field` will be used if `deleted_flg` is defined which tells us when exactly it was deleted from source. *null* is used for effective records but you can specify a timestamp in `max_dttm`. Fields that hold date intervals are called *effective_from_dttm* and *effective_to_dttm* but that can be changed with `start_field` and `end_field`.
Seeds

*Timestamp* strategy can be used to load data from CDC source. If you load data from table containing Change Data Capture events like Insert, Update, Delete, `dbt-light` will handle situations like multiple versions for the same row in one portion of data and deletions based on Delete events and not on the absence of a row. In order for it to work you must specify `deleted_flg` where `dbt-light` will search for Delete events. By default it will check for 'Delete' values but that can be changed with `deleted_flg_val` property. You might also need to set `invalidate_hard_deletes` to False so it won't invalidate rows based on the absence of it in data portion. `deleted_flg` can be used not only for CDC data but for invalidating rows based on some value, for example, if you have *is_active* column which if set to *N* means it was deleted from source and should be invalidated in snapshot instead of creating new version.   

Seeds
-------------------------------------------------------------------------------

`Seeds` are csv files containing static data that rarely changes. Configuration is specified in *seeds.yaml* but schema is implied by the subfolder in *seeds/*. By default comma is used as a delimeter but it can be changed in `delimiter` property. 

Column names and types should be specified under columns for each seed:

```yaml
seeds:
  - name: seed
    columns:
      - name: country_code
        type: varchar(2)
      - name: country_name
        type: varchar(32)
```

Sources
-------------------------------------------------------------------------------

`Sources` are tables containing data loaded by your DI tools. They're are used to interpolate source schema in models that reference them. Each source has `name`, `schema` and `tables`. Optionally `quoting` can be specified either for source or specific table which is False by default. 

You can use {{ some_source.some_table }} to reference source in the example below: 

```yaml
sources:
  - name: some_source
    schema: etl_stg
    tables:
      - name: some_table
```

Tests
-------------------------------------------------------------------------------

`Tests` are assertions that you make about your data. They're used instead of constraints to alert you that something went wrong and possibly make rollback. Tests are executed right after object is materialized and if `on_test_fail` is *error_with_rollback* rollback is done or not if it's just *error*. 

Tests can be defined for `Models`, `Snapshots` and `Seeds` in `columns` key:

```yaml
models:
  - name: some_table
    on_test_fail: error_with_rollback
    columns:
      - name: some_id
        tests:
          - not_null
          - unique
      - name: some_type
        tests:
          - accepted_values:
              values: ["1type", "2type"]
      - name: some_fk
        tests:
          - relationships:
              to: other_model
              field: other_id
```

All existing tests are showed in the example above. Custom tests are not yet supported. 

Macros
-------------------------------------------------------------------------------

You can use jinja macros in models/snapshots. They're defined as usual jinja macros and stored in *macros/*:

```sql
{% macro surrogate_key(bus_key, sur_key) -%}
    case when {{ bus_key }} is not null then source_system_cd || '_' || row_id else null end as {{ sur_key }}
{%- endmacro %}
```

Statement
-------------------------------------------------------------------------------

Function `statement` is a way to perform select query from template and return results back to jinja context. Statement will return either a list if only one column was selected or list of tuples where tuple represents a row:

```sql
{% set country_codes = statement('select country_code from {{ some_seed }}') %}

select * from {{ some_model }} where country_code in (
{% for cc in country_codes %}
	'{{ cc }}'
	{{ ', ' if not loop.last }}
{% endfor %})
```


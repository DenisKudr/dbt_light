import datetime
import os

import yaml
from pathlib import Path
from typing import Literal, Union
from schema import Schema, Optional, SchemaError, Or
from dbt_light.exceptions import ConfigReadError, ConfigValidateError, SnapshotNotFound, DuplicateSnapshotsError, \
    DBTProjectNotFound


def config_read(dbt_project: str = None, config_type: Literal['profiles', 'project', 'snapshots'] = 'project',
                snapshot_name: str = None) -> Union[dict, list]:

    profiles_path = f'/home/{os.getlogin()}/.dbt_light/profiles.yaml'
    try:
        profiles = yaml.safe_load(Path(profiles_path).read_text())
    except (FileNotFoundError, OSError, yaml.YAMLError) as er:
        raise ConfigReadError(profiles_path) from er

    profiles_schema = Schema({
        str: {
            'adapter': 'postgres',
            'path': str,
            'host': str,
            'port': int,
            'dbname': str,
            'user': str,
            'pass': str
        }
    })

    try:
        profiles = profiles_schema.validate(profiles)
    except SchemaError as er:
        raise ConfigValidateError(profiles_path) from er

    dbt_projects = list(profiles.keys())
    if len(dbt_projects) == 0:
        raise DBTProjectNotFound(f'No dbt_light projects specified in {profiles_path}')
    elif len(dbt_projects) > 1 and not dbt_project:
        raise DBTProjectNotFound(f'More than one project found in {profiles_path}.\nYou should pass dbt_project param')
    elif len(dbt_projects) == 1:
        dbt_profile = profiles[dbt_projects[0]]
    else:
        try:
            dbt_profile = profiles[dbt_project]
        except KeyError as er:
            raise DBTProjectNotFound(f'No dbt_light project {dbt_project} specified in {profiles_path}') from er

    dbt_project_folder = dbt_profile['path']
    if not Path(dbt_project_folder).is_dir():
        raise DBTProjectNotFound(f'Dbt_light project path for project {dbt_project} is not dir')

    if config_type == 'profiles':
        return dbt_profile
    else:
        config_path = f'{dbt_project_folder}/{config_type}.yaml'
    try:
        config = yaml.safe_load(Path(config_path).read_text())
    except (FileNotFoundError, OSError, yaml.YAMLError) as er:
        raise ConfigReadError(config_path) from er

    project_config_schema = Schema({
        Optional('models'): {
            Optional('views'): [
                {
                    'schema': str,
                    Optional('pattern'): str,
                    Optional('models'): [str]
                }
            ],
            Optional('seq_keys'): [
                {
                    'model': str,
                    'name': str
                }
            ]
        }
    })

    snapshot_config_schema = Schema({
        'target_schema': str,
        'snapshot': str,
        'key_fields': Or(str, [str]),
        'strategy': Or('check', 'timestamp'),
        Optional('source_schema'): str,
        Optional('input_table'): str,
        Optional('delta_schema'): str,
        Optional('delta_table', default='temp_delta_table'): str,
        Optional('invalidate_hard_deletes', default=True): bool,
        Optional('max_dttm'): Or(datetime.date, datetime.datetime),
        Optional('updated_at_field', default=''): str,
        Optional('hash_diff_field', default='HASH_DIFF'): str,
        Optional('processed_field', default='PROCESSED_DTTM'): str,
        Optional('start_field', default='effective_from_dttm'): str,
        Optional('end_field', default='effective_to_dttm'): str,
        Optional('data_fields'): Or(str, [str]),
        Optional('ignored_data_fields'): Or(str, [str])
    })

    if config_type == 'snapshots':
        if snapshot_name:
            snapshot = list(filter(lambda x: x['name'] == snapshot_name, config['snapshots']))
            if len(snapshot) == 0:
                raise SnapshotNotFound(snapshot_name, config_path)
            if len(snapshot) > 1:
                raise DuplicateSnapshotsError(snapshot_name, config_path)

            snapshot[0]['snapshot'] = snapshot[0].pop('name')
            config = {key: value for key, value in config.items() if key != 'snapshots'}
            config.update(snapshot[0])
            try:
                validated_config = snapshot_config_schema.validate(config)
            except SchemaError as er:
                raise ConfigValidateError(config_path) from er
        else:
            validated_config = []
            for snapshot in config['snapshots']:
                snapshot['snapshot'] = snapshot.pop('name')
                snapshot_config = {key: value for key, value in config.items() if key != 'snapshots'}
                snapshot_config.update(snapshot)
                try:
                    validated_snapshot_config = snapshot_config_schema.validate(snapshot_config)
                except SchemaError as er:
                    raise ConfigValidateError(config_path) from er
                validated_config.append(validated_snapshot_config)
    else:
        try:
            validated_config = project_config_schema.validate(config)
        except SchemaError as er:
            raise ConfigValidateError(config_path) from er

    return validated_config

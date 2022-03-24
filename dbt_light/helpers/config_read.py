import datetime
import yaml
from pathlib import Path
from typing import Literal, Union
from schema import Schema, Optional, SchemaError, Or
from dbt_light.exceptions import ConfigReadError, ConfigValidateError, SnapshotNotFound, DuplicateSnapshotsError


def config_read(dbt_project_folder: str, config_type: Literal['project', 'snapshots'],
                snapshot_name: str = None) -> Union[dict, list]:

    config_path = f'{dbt_project_folder}/{config_type}.yaml'
    try:
        config = yaml.safe_load(Path(config_path).read_text())
    except (FileNotFoundError, OSError, yaml.YAMLError) as er:
        raise ConfigReadError(config_path) from er

    project_config_schema = Schema({
        'db_adapter': 'postgres',
        'db_server': str,
        'db_name': str,
        'db_user': str,
        'db_password': str,
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

import datetime
import re
from pathlib import Path
from typing import Union
import yaml
from schema import Schema, SchemaError, Or, Optional
from dbt_light.exceptions import ConfigReadError, ConfigValidateError, DuplicateSnapshotsError, NoInputSpecifiedError, \
    ModelReadError


class SnapshotContext:
    def __init__(self, dbt_project_path: str):
        self.dbt_project_path = dbt_project_path
        self.snapshots_config_path = f"{dbt_project_path}/snapshots.yaml"
        self.config = None
        self.snapshots = None
        try:
            self.config = yaml.safe_load(Path(self.snapshots_config_path).read_text())
        except FileNotFoundError:
            pass
        except (OSError, yaml.YAMLError) as er:
            raise ConfigReadError(self.snapshots_config_path) from er
        if self.config:
            self.snapshots = self.validate_config()

    def validate_config(self) -> dict:
        snapshot_schema = Schema({
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

        validated_snapshots = {}

        for snapshot in [snap for snap in self.config['snapshots'] if not snap.get('pattern_name')]:
            snapshot['snapshot'] = snapshot.pop('name')
            snapshot_config = {key: value for key, value in self.config.items() if key != 'snapshots'}
            for pattern in list(filter(lambda x: x.get('pattern_name'), self.config['snapshots'])):
                if re.search(pattern['pattern_name'], snapshot['snapshot']):
                    snapshot_config.update({key: value for key, value in pattern.items() if key != 'pattern_name'})
            snapshot_config.update(snapshot)
            try:
                validated_snapshot_config = snapshot_schema.validate(snapshot_config)
            except SchemaError as er:
                raise ConfigValidateError(self.snapshots_config_path) from er
            if not validated_snapshots.get(snapshot['snapshot']):
                validated_snapshots.update({
                    snapshot['snapshot']: validated_snapshot_config
                })
            else:
                raise DuplicateSnapshotsError(snapshot['snapshot'], self.snapshots_config_path)

        return validated_snapshots

    def get_snapshot(self, snapshot_name: str) -> Union[dict, None]:
        snapshot = None
        if self.snapshots:
            snapshot = self.snapshots.get(snapshot_name)
            if not snapshot:
                return None

            if not snapshot.get('input_table'):
                snapshot_file = Path(f"{self.dbt_project_path}/snapshots/{snapshot['snapshot']}.sql")
                if not snapshot_file.is_file():
                    raise NoInputSpecifiedError(snapshot['snapshot'])
                try:
                    model = snapshot_file.read_text()
                except OSError as er:
                    raise ModelReadError(snapshot_file) from er
                snapshot['model_sql'] = model
                snapshot['input_table'] = 'temp_' + snapshot['snapshot']
            if snapshot.get('max_dttm'):
                snapshot.update({'max_dttm': snapshot.get('max_dttm').strftime('%Y-%m-%d %H:%M:%S')})

            snapshot.update({key: [value] for key, value in snapshot.items() if
                            key in ['key_fields', 'ignored_data_fields', 'data_fields'] and type(value) == str})
        return snapshot

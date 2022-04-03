import re
from glob import glob
from pathlib import Path
from typing import Union
import yaml
from schema import Schema, Optional, Or, SchemaError
from dbt_light.exceptions import ConfigReadError, DuplicateSeedsError, ConfigValidateError


class SeedContext:
    def __init__(self, dbt_project_path: str):
        self.dbt_project_path = dbt_project_path
        self.seeds_config_path = f"{dbt_project_path}/seeds.yaml"
        self.config = None
        try:
            self.config = yaml.safe_load(Path(self.seeds_config_path).read_text())
        except FileNotFoundError:
            pass
        except (OSError, yaml.YAMLError) as er:
            raise ConfigReadError(self.seeds_config_path) from er
        self.seeds = self.find_seeds() if self.config else {}

    def find_seeds(self) -> dict:
        seeds_schemas = [f for f in Path(f"{self.dbt_project_path}/seeds/").iterdir() if f.is_dir()]
        seeds_paths = {key.name: glob(str(key) + '/*.csv') for key in seeds_schemas}

        seeds = {}
        for schema, seed_paths in seeds_paths.items():
            for seed_path in seed_paths:
                seed_name = Path(seed_path).stem
                if not seeds.get(seed_name):
                    seeds.update({
                        seed_name: {
                            'seed': seed_name,
                            'target_schema': schema,
                            'path': seed_path,
                            'delimiter': ','
                        }
                    })
                else:
                    raise DuplicateSeedsError(seed_name, [seeds[seed_name], schema])

        config_seeds = {}
        config = self.validate_config()
        for seed in seeds.values():
            seed_config = seed
            seed_config['tests'] = {}
            seed_config.update({key: value for key, value in config.items() if key != 'seeds'})

            for pattern in list(filter(lambda x: x.get('pattern_name'), config['seeds'])):
                if re.search(pattern['pattern_name'], seed['seed']):
                    seed_config.update({key: value for key, value in pattern.items() if key != 'pattern_name'})
                    if pattern.get('columns'):
                        seed_config['tests'] = {col['name']: col['tests'] for col in pattern['columns']}

            for seed_in_config in config['seeds']:
                if seed_in_config.get('name') == seed['seed']:
                    seed_config.update({key: value for key, value in seed_in_config.items()
                                        if key not in ('name', 'columns')})

                    for col in seed_in_config['columns']:
                        seed_config['columns'] = {col['name']: col['type'] for col in seed_in_config['columns']}
                        if seed_config['tests'].get(col['name']) and col.get('tests'):
                            seed_config['tests'][col['name']] = seed_config['tests'][col['name']] + col['tests']
                        elif col.get('tests'):
                            seed_config['tests'][col['name']] = col['tests']

            if not config_seeds.get(seed_config['seed']):
                config_seeds.update({
                    seed_config['seed']: seed_config
                })
            else:
                raise DuplicateSeedsError(seed_config['seed'], [seed_config['model_schema'],
                                                                seed_config['config_models']])
        return config_seeds

    def validate_config(self) -> dict:
        seed_schema = Schema({
            Optional('delimiter', default=','): str,
            Optional('on_test_fail', default='error'): Or('error', 'error_with_rollback'),
            Optional('seeds'): [{
                Or("name", "pattern_name", only_one=True): str,
                Optional('delimiter', default=','): str,
                Optional('on_test_fail', default='error'): Or('error', 'error_with_rollback'),
                'columns': [{
                    'name': str,
                    'type': str,
                    Optional('description'): str,
                    Optional('tests'): [
                        Or(str, dict)
                    ]
                }]
            }]
        })
        try:
            validated_seed_config = seed_schema.validate(self.config)
        except SchemaError as er:
            raise ConfigValidateError(self.seeds_config_path) from er
        return validated_seed_config

    def get_seed(self, seed_name: str) -> Union[dict, None]:
        seed = None
        if self.seeds:
            seed = self.seeds.get(seed_name)
            if not seed:
                return None
        return seed

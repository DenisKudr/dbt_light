import re
from glob import glob
from pathlib import Path
from typing import Union
import yaml
from schema import Schema, Or, Optional, SchemaError

from dbt_light.exceptions import ConfigReadError, DuplicateModelsError, ConfigValidateError, ModelReadError


class ModelContext:
    def __init__(self, dbt_project_path: str):
        self.dbt_project_path = dbt_project_path
        self.models_config_path = f"{dbt_project_path}/models.yaml"
        self.config = None
        self.models = None
        try:
            self.config = yaml.safe_load(Path(self.models_config_path).read_text())
        except FileNotFoundError:
            pass
        except (OSError, yaml.YAMLError) as er:
            raise ConfigReadError(self.models_config_path) from er
        self.models = self.find_models()

    def find_models(self) -> Union[dict, None]:
        models_schemas = [f for f in Path(f"{self.dbt_project_path}/models/").iterdir() if f.is_dir()]
        incr_models_schemas = [f for f in Path(f"{self.dbt_project_path}/incremental_models/").iterdir() if
                               f.is_dir()]

        models_paths = {key.name: glob(str(key) + '/*.sql') for key in models_schemas}
        incr_models_paths = {key.name: glob(str(key) + '/*.sql') for key in incr_models_schemas}

        for schema, models_list in incr_models_paths.items():
            if models_paths.get(schema):
                models_paths.update({schema: models_paths.get(schema) + models_list})
            else:
                models_paths.update({schema: models_list})
        if not models_paths:
            return None

        models = {}
        for schema, models_paths in models_paths.items():
            for model_path in models_paths:
                model_name = Path(model_path).stem
                if not models.get(model_name):
                    models.update({
                        model_name: {
                            'model': model_name,
                            'target_schema': schema,
                            'path': model_path,
                            'is_incremental': True if 'incremental_models' in model_path else False,
                            'materialization': 'table'
                        }
                    })
                else:
                    raise DuplicateModelsError(model_name, [models[model_name], schema])

        if self.config:
            config_models = {}
            config = self.validate_config()
            for model in models.values():
                model_config = model
                model.update({key: value for key, value in config.items() if key != 'models'})

                for pattern in list(filter(lambda x: x.get('pattern_name'), config['models'])):
                    if re.search(pattern['pattern_name'], model['model']):
                        model_config.update({key: value for key, value in pattern.items() if key != 'pattern_name'})

                for model_in_config in config['models']:
                    if model_in_config.get('name') == model['model']:
                        model_config.update({key: value for key, value in model_in_config.items() if key != 'name'})

                if not config_models.get(model_config['model']):
                    config_models.update({
                        model_config['model']: model_config
                    })
                else:
                    raise DuplicateModelsError(model_config['model'], [model_config['model_schema'],
                                                                       model_config['config_models']])
            return config_models
        return models

    def validate_config(self) -> dict:
        models_schema = Schema({
            Optional('materialization', default='table'): Or('table', 'view'),
            Optional('incr_key'): str,
            Optional('models'): [
                {
                    Or("name", "pattern_name", only_one=True): str,
                    Optional('materialization'): str,
                    Optional('incr_key'): str
                }
            ]
        })
        try:
            validated_model_config = models_schema.validate(self.config)
        except SchemaError as er:
            raise ConfigValidateError(self.models_config_path) from er
        return validated_model_config

    def get_model(self, model_name: str) -> Union[dict, None]:
        model = None
        if self.models:
            model = self.models.get(model_name)
            if not model:
                return None

            try:
                model_sql = Path(model['path']).read_text()
            except OSError as er:
                raise ModelReadError(model['path']) from er
            model.update({
                'model_sql': model_sql
            })
        return model

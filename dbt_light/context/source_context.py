from pathlib import Path
import yaml
from schema import Schema, Optional, SchemaError
from dbt_light.exceptions import ConfigReadError, ConfigValidateError, DuplicateSourcesError, \
    DuplicateTablesInSourceError


class SourceContext:
    def __init__(self, dbt_project_path: str):
        self.dbt_project_path = dbt_project_path
        self.source_config_path = f"{dbt_project_path}/sources.yaml"
        self.config = None
        try:
            self.config = yaml.safe_load(Path(self.source_config_path).read_text())
        except FileNotFoundError:
            pass
        except (OSError, yaml.YAMLError) as er:
            raise ConfigReadError(self.source_config_path) from er
        self.sources = self.validate_config() if self.config else {}

    def validate_config(self) -> dict:
        source_schema = Schema({
            'sources': [{
                'name': str,
                'schema': str,
                Optional('description'): str,
                Optional('quoting', default=False): bool,
                'tables': [{
                    'name': str,
                    Optional('description'): str,
                    Optional('quoting'): bool
                }]
            }]
        })
        try:
            validated_seed_config = source_schema.validate(self.config)
        except SchemaError as er:
            raise ConfigValidateError(self.source_config_path) from er

        sources = {}
        for source in validated_seed_config['sources']:
            source_config = {key: value for key, value in source.items() if key != 'tables'}
            source_name = source_config['name']
            if not sources.get(source_name):
                sources.update({
                    source_config['name']: {}
                })
                for table in source['tables']:
                    if not sources[source_name].get(table['name']):
                        if 'quoting' in table:
                            quoting = table['quoting']
                        else:
                            quoting = source_config['quoting']
                        sources[source_name].update({
                            table['name']: f"{source_config['schema']}.\"{table['name']}\""
                            if quoting else f"{source_config['schema']}.{table['name']}"
                        })
                    else:
                        raise DuplicateTablesInSourceError(table['name'], source_name, self.source_config_path)
            else:
                raise DuplicateSourcesError(source_config['name'], self.source_config_path)
        return sources

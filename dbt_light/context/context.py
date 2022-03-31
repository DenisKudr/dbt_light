import os
from glob import glob
from pathlib import Path
import yaml
from schema import Schema, SchemaError
from dbt_light.context.seed_context import SeedContext
from dbt_light.context.snapshot_context import SnapshotContext
from dbt_light.context.model_context import ModelContext
from dbt_light.exceptions import ConfigReadError, ConfigValidateError, DBTProjectNotFound, DuplicateModelsError, \
    ModelRenderError
from jinja2 import TemplateError, TemplateSyntaxError
from jinja2.nativetypes import NativeEnvironment


class Context:
    def __init__(self, dbt_project: str = None):
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
        if len(dbt_projects) > 1 and not dbt_project:
            raise DBTProjectNotFound(f'More than one project found in {profiles_path}.\n'
                                     f'You should pass dbt_project param')
        elif len(dbt_projects) == 1:
            dbt_profile = profiles[dbt_projects[0]]
        else:
            try:
                dbt_profile = profiles[dbt_project]
            except KeyError as er:
                raise DBTProjectNotFound(f'No dbt_light project {dbt_project} specified in {profiles_path}') from er

        if not Path(dbt_profile['path']).is_dir():
            raise DBTProjectNotFound(f'dbt_light project path for project {dbt_project} is not a dir')

        self.dbt_profile = dbt_profile
        self.model_context = ModelContext(self.dbt_profile['path'])
        self.snapshot_context = SnapshotContext(self.dbt_profile['path'])
        self.seed_context = SeedContext(self.dbt_profile['path'])

    def schemas_context(self) -> dict:
        models = self.model_context.models
        snapshots = self.snapshot_context.snapshots
        seeds = self.seed_context.seeds
        delta_tables = {value['delta_table']: {'target_schema': value['delta_schema']} for value in
                        [snap for snap in self.snapshot_context.snapshots.values()
                         if snap['delta_table'] != 'temp_delta_table']}

        schemas_context = {}
        for entity_dict in [models, snapshots, seeds, delta_tables]:
            for entity_key, entity_value in entity_dict.items():
                if not schemas_context.get(entity_key):
                    schemas_context.update({
                        entity_key: f"{entity_value['target_schema']}.{entity_key}"
                    })
                else:
                    raise DuplicateModelsError(entity_key, [entity_value['target_schema'],
                                                            schemas_context.get(entity_key).split('.')[0]])

        return schemas_context

    def macro_context(self, model: str) -> str:
        macros_paths = glob(f"{self.dbt_profile['path']}/macros/*.sql")
        macros_sql = []
        for macro in macros_paths:
            macro_sql = Path(macro).read_text()
            macros_sql.append(macro_sql)
        macros = '\n'.join(macros_sql)
        return macros + model

    def render_model(self, model: str, context: dict = None) -> str:
        schemas_context = self.schemas_context()
        model_with_macros = self.macro_context(model)
        template = NativeEnvironment().from_string(model_with_macros)
        if context:
            schemas_context.update(context)
        try:
            rendered = template.render(schemas_context)
        except (TemplateError, TemplateSyntaxError) as er:
            raise ModelRenderError(model) from er

        return rendered

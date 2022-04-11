import os
from glob import glob
from pathlib import Path
import yaml
from schema import Schema, SchemaError
from dbt_light.context.seed_context import SeedContext
from dbt_light.context.snapshot_context import SnapshotContext
from dbt_light.context.model_context import ModelContext
from dbt_light.context.source_context import SourceContext
from dbt_light.exceptions import ConfigReadError, ConfigValidateError, DBTProjectNotFound, DuplicateModelsError, \
    ModelRenderError, MacroNotFound, DuplicateMacroError
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
            dbt_project = dbt_projects[0]
            dbt_profile = profiles[dbt_project]
        else:
            try:
                dbt_profile = profiles[dbt_project]
            except KeyError as er:
                raise DBTProjectNotFound(f'No dbt_light project {dbt_project} specified in {profiles_path}') from er

        if not Path(dbt_profile['path']).is_dir():
            raise DBTProjectNotFound(f'dbt_light project path for project {dbt_project} is not a dir')

        self.dbt_profile = dbt_profile
        self.dbt_project = dbt_project
        self.model_context = ModelContext(self.dbt_profile['path'])
        self.snapshot_context = SnapshotContext(self.dbt_profile['path'])
        self.seed_context = SeedContext(self.dbt_profile['path'])
        self.source_context = SourceContext(self.dbt_profile['path'])
        self.env = self.create_jinja_env()

    def create_jinja_env(self) -> NativeEnvironment:
        env = NativeEnvironment()
        env.globals.update(self.schemas_context())
        env.globals.update(self.macro_context(env))
        env.globals['statement'] = self.statement
        return env

    def schemas_context(self) -> dict:
        models = self.model_context.models
        snapshots = self.snapshot_context.snapshots
        seeds = self.seed_context.seeds
        sources = self.source_context.sources
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
        schemas_context.update(sources)

        return schemas_context

    def macro_context(self, env: NativeEnvironment) -> dict:
        macros_paths = glob(f"{self.dbt_profile['path']}/macros/*.sql")
        macros = {}
        for macro in macros_paths:
            macro_name = Path(macro).stem
            macro_sql = Path(macro).read_text()
            macro_template = env.from_string(macro_sql)
            if not macros.get(macro_name):
                try:
                    macros[macro_name] = getattr(macro_template.module, macro_name)
                except AttributeError:
                    raise MacroNotFound(macro_name, macro)
            else:
                raise DuplicateMacroError(macro_name, macro)
        return macros

    def render_model(self, model: str, context: dict = None, conn = None) -> str:
        self.env.globals['conn'] = conn
        template = self.env.from_string(model)
        try:
            rendered = template.render(context)
        except (TemplateError, TemplateSyntaxError) as er:
            raise ModelRenderError(model) from er
        return rendered

    def statement(self, query: str) -> list:
        template = self.env.from_string(query)
        rendered = template.render()
        res = self.env.globals['conn'].query(rendered)
        if len(res) > 0 and len(res[0]) == 1:
            res = [row[0] for row in res]
        return res

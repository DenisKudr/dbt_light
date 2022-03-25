import re
from glob import glob
from pathlib import Path
from dbt_light.exceptions import DuplicateModelsError, ModelNotFound, DBTProjectNotFound, ModelReadError, \
    NoInputSpecifiedError
from dbt_light.helpers.config_read import config_read


def model_search(dbt_project_folder: str, model_name: str = None, snapshot_name: str = None) -> dict:

    if not Path(dbt_project_folder).is_dir():
        raise DBTProjectNotFound(dbt_project_folder)

    if snapshot_name:
        snapshot = config_read(dbt_project_folder, 'snapshots', snapshot_name)
        if not snapshot.get('input_table'):
            snapshot_file = Path(f"{dbt_project_folder}/snapshots/{snapshot['snapshot']}.sql")
            if not snapshot_file.is_file():
                raise NoInputSpecifiedError(snapshot['snapshot'])
            try:
                model = snapshot_file.read_text()
            except OSError as er:
                raise ModelReadError(snapshot_file) from er
            snapshot['model'] = model

        return snapshot

    else:
        models_schemas = [f for f in Path(f'{dbt_project_folder}/models/').iterdir() if f.is_dir()]
        models = {key.name: glob(str(key) + '/*.sql') for key in models_schemas}

        incr_models_schemas = [f for f in Path(f'{dbt_project_folder}/incremental_models/').iterdir() if f.is_dir()]
        incr_models = {key.name: glob(str(key) + '/*.sql') for key in incr_models_schemas}
        for key, value in incr_models.items():
            if models.get(key):
                models.update({key: models.get(key) + value})
            else:
                models.update({key: value})

        snapshots = config_read(dbt_project_folder, 'snapshots')
        snap_schemas = {}
        for snap in snapshots:
            if snap_schemas.get(snap['target_schema']):
                snap_schemas[snap['target_schema']] = snap_schemas[snap['target_schema']] + [snap['snapshot']]
            else:
                snap_schemas[snap['target_schema']] = [snap['snapshot']]

        for key, value in snap_schemas.items():
            if models.get(key):
                models.update({key: models.get(key) + value})
            else:
                models.update({key: value})

        if not model_name:
            schemas_context = {}
            for schema, models in models.items():
                for model in models:
                    if not schemas_context.get(Path(model).stem):
                        schemas_context.update({Path(model).stem: f'{schema}.{Path(model).stem}'})
                    else:
                        raise DuplicateModelsError(model_name, [schemas_context.get(Path(model).stem), schema])
            return schemas_context

        else:
            model = {}
            for schema, models in models.items():
                model_names = {Path(model).stem: model for model in models}
                if model_names.get(model_name):
                    if not model:
                        model.update({
                            'model_schema': schema,
                            'path': str(model_names[model_name]),
                            'is_incremental': True if 'incremental_models' in str(model_names[model_name]) else False
                        })
                    else:
                        raise DuplicateModelsError(model_name, [model[schema], schema])
            if not model:
                raise ModelNotFound(model_name)

            try:
                model_sql = Path(model['path']).read_text()
            except OSError as er:
                raise ModelReadError(model['path']) from er

            model.update({
                'model': model_sql,
                'model_name': model_name
            })

            project_config = config_read(dbt_project_folder, 'project')

            model.update({'materialization': 'table'})
            views = project_config.get('views')
            if views:
                for view in views:
                    if view['schema'] == model['model_schema']:
                        if not view.get('pattern') and not view.get('models'):
                            model['materialization'] = 'view'
                        if (view.get('pattern') and re.search(view['pattern'], model_name)) or \
                                (view.get('models') and model_name in view.get('models')):
                            model['materialization'] = 'view'

            model.update({'seq_key': None})
            seq_keys = project_config.get('seq_keys')
            if model['is_incremental'] and seq_keys:
                for key in seq_keys:
                    if key['model'] == model_name:
                        model['seq_key'] = key['name']

            return model

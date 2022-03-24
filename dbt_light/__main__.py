import os
import yaml

os.mkdir(os.getcwd() + '/dbtLightProject')
os.mkdir(os.getcwd() + '/dbtLightProject/models')
os.mkdir(os.getcwd() + '/dbtLightProject/incremental_models')
os.mkdir(os.getcwd() + '/dbtLightProject/snapshots')

project_yaml = {
    'db_adapter': '',
    'db_server': '',
    'db_name': '',
    'db_user': '',
    'db_password': '',
    'views': [
        {
            'schema': '',
            'pattern': '',
            'models': []
        }
    ],
    'seq_keys': [
        {
            'model': '',
            'name': ''
        }
    ]
}

snapshots_yaml = {
    "target_schema": '',
    "source_schema": "",
    "delta_schema": "",
    "snapshots": []
}

with open(os.getcwd() + '/dbtLightProject/project.yaml', 'w') as file:
    yaml.dump(project_yaml, file, default_flow_style=False, default_style=None)
with open(os.getcwd() + '/dbtLightProject/snapshots.yaml', 'w') as file:
    yaml.dump(snapshots_yaml, file, default_flow_style=False, default_style=None)

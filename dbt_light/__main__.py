import os
import yaml
from pathlib import Path

profiles_dir = str(Path.home()) + '/.dbt_light'
cur_dir = os.getcwd() + '/dbt_light_project'

if not os.path.exists(profiles_dir):
    os.mkdir(profiles_dir)

profiles_path = Path(profiles_dir + '/profiles.yaml')

if not profiles_path.is_file():
    profiles = {
        'dbt_light_project': {
            'path': cur_dir,
            'adapter': 'postgres',
            'host': 'localhost',
            'port': 5432,
            'user': 'postgres',
            'pass': 'pass',
            'dbname': 'postgres'
        }
    }

    with open(str(profiles_path), 'w') as file:
        yaml.dump(profiles, file, default_flow_style=False, default_style=None)
else:
    profiles = {
        'dbt_light_project': {
            'path': cur_dir,
            'adapter': 'postgres',
            'host': 'localhost',
            'port': 5432,
            'user': 'postgres',
            'pass': 'pass',
            'dbname': 'postgres'
        }
    }
    with open(str(profiles_path), 'a') as file:
        yaml.dump(profiles, file, default_flow_style=False, default_style=None)

Path(cur_dir).mkdir(exist_ok=True)
Path(cur_dir + '/models').mkdir(exist_ok=True)
Path(cur_dir + '/incremental_models').mkdir(exist_ok=True)
Path(cur_dir + '/snapshots').mkdir(exist_ok=True)
Path(cur_dir + '/seeds').mkdir(exist_ok=True)
Path(cur_dir + '/macros').mkdir(exist_ok=True)
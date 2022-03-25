from unittest import TestCase, main
from dbt_light.db_connection.database_connection import DatabaseConnection
from dbt_light.snapshot import Snapshot
import os


class TestScd2Loader(TestCase):

    @classmethod
    def setUpClass(cls):
        dbt_test_project_folder = os.environ['dbt_test_project_folder']
        cls.dbt_test_project_folder = dbt_test_project_folder
        cls.test_params = [
            {'snapshot': 'snap_check'},
            {'snapshot': 'snap_timestamp'},
            {'snapshot': 'snap_check_no_delta_table'},
            {'snapshot': 'snap_check_processed'},
            {'snapshot': 'snap_check_no_data', 'init_script': 'snapshot_init_no_data_fields',
             'incr_script': 'snapshot_incr_no_data_fields'},
            {'snapshot': 'snap_check_new_fields', 'incr_script': 'snapshot_incr_new_fields'},
            {'snapshot': 'snap_check_with_model'}
        ]
        with DatabaseConnection(dbt_test_project_folder) as db:
            with open(f"sql/{db.config['db_adapter']}/snapshot_init_drop.sql", 'r') as f:
                setup = f.read()
            db.execute(setup)

    def execute_test(self, **kwargs):

        snapshot = kwargs.get('snapshot')
        init_script = kwargs.get('init_script', 'snapshot_init')
        incr_script = kwargs.get('incr_script', 'snapshot_incr')
        print(f"Executing test for {snapshot}")
        print(f"Initializing with {init_script}")

        with DatabaseConnection(self.dbt_test_project_folder) as db:
            with open(f"sql/{db.config['db_adapter']}/{init_script}.sql", 'r') as f:
                setup = f.read()
            db.execute(setup)
        snap = Snapshot(self.dbt_test_project_folder, snapshot)
        snap.materialize()

        print(f"Initializing increment with {incr_script}")
        with DatabaseConnection(self.dbt_test_project_folder) as db:
            with open(f"sql/{db.config['db_adapter']}/{incr_script}.sql", 'r') as f:
                setup = f.read()
            db.execute(setup)
        snap = Snapshot(self.dbt_test_project_folder, snapshot)
        snap.materialize()

    def test_snap(self):
        for test in self.test_params:
            with self.subTest(test['snapshot']):
                self.execute_test(**test)
                # TODO: add asserts


if __name__ == '__main__':
    main()

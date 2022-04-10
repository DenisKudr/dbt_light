from unittest import TestCase, main
from dbt_light.db_connection.database_connection import DatabaseConnection
from dbt_light.snapshot import Snapshot


class TestSnapshot(TestCase):

    @classmethod
    def setUpClass(cls):
        dbt_test_project = 'dbt_test_project'
        cls.dbt_test_project = dbt_test_project
        cls.test_params = [
            {'snapshot': 'snap_check'},
            {'snapshot': 'snap_timestamp'},
            {'snapshot': 'snap_check_no_delta_table'},
            {'snapshot': 'snap_check_processed'},
            {'snapshot': 'snap_check_no_data', 'init_script': 'snapshot_init_no_data_fields',
             'incr_script': 'snapshot_incr_no_data_fields'},
            {'snapshot': 'snap_check_new_fields', 'incr_script': 'snapshot_incr_new_fields'},
            {'snapshot': 'snap_check_with_model'},
            {'snapshot': 'snap_check', 'full_refresh': True},
            {'snapshot': 'snap_timestamp_cdc', 'init_script': 'snapshot_init_cdc',
             'incr_script': 'snapshot_incr_cdc'}
        ]
        with DatabaseConnection(dbt_test_project) as db:
            with open(f"sql/{db.config['adapter']}/snapshot_init_drop.sql", 'r') as f:
                setup = f.read()
            db.execute(setup)

    def execute_test(self, **kwargs):

        snapshot = kwargs.get('snapshot')
        init_script = kwargs.get('init_script', 'snapshot_init')
        incr_script = kwargs.get('incr_script', 'snapshot_incr')
        full_refresh = kwargs.get('full_refresh', False)
        print(f"Executing test for {snapshot}")
        print(f"Initializing with {init_script}")

        with DatabaseConnection(self.dbt_test_project) as db:
            with open(f"sql/{db.config['adapter']}/{init_script}.sql", 'r') as f:
                setup = f.read()
            db.execute(setup)
        snap = Snapshot(snapshot, self.dbt_test_project, full_refresh=full_refresh)
        snap.materialize()

        print(f"Initializing increment with {incr_script}")
        with DatabaseConnection(self.dbt_test_project) as db:
            with open(f"sql/{db.config['adapter']}/{incr_script}.sql", 'r') as f:
                setup = f.read()
            db.execute(setup)
        snap = Snapshot(snapshot, self.dbt_test_project)
        snap.materialize()

    def test_snap(self):
        for test in self.test_params:
            with self.subTest(test['snapshot']):
                self.execute_test(**test)
                # TODO: add asserts


if __name__ == '__main__':
    main()

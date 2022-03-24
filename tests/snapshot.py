from unittest import TestCase, main
from dbt_light.db_connection.database_connection import DatabaseConnection
from dbt_light.snapshot import Snapshot
import os


class TestScd2Loader(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.dbt_test_project_folder = os.environ['dbt_test_project_folder']

    def test_check(self):

        with DatabaseConnection(self.dbt_test_project_folder) as db:
            with open(f"sql/{db.config['db_adapter']}/snapshot_init.sql", 'r') as f:
                setup = f.read()
            db.execute(setup)
        snap = Snapshot('snap_check', self.dbt_test_project_folder)
        snap.build()

        with DatabaseConnection(self.dbt_test_project_folder) as db:
            with open(f"sql/{db.config['db_adapter']}/snapshot_incr.sql", 'r') as f:
                setup = f.read()
            db.execute(setup)
        snap = Snapshot('snap_check', self.dbt_test_project_folder)
        snap.delta_calc()
        snap.delta_apply()

    def test_timestamp(self):

        with DatabaseConnection(self.dbt_test_project_folder) as db:
            with open(f"sql/{db.config['db_adapter']}/snapshot_init.sql", 'r') as f:
                setup = f.read()
            db.execute(setup)
        snap = Snapshot('snap_timestamp', self.dbt_test_project_folder)
        snap.build()

        with DatabaseConnection(self.dbt_test_project_folder) as db:
            with open(f"sql/{db.config['db_adapter']}/snapshot_incr.sql", 'r') as f:
                setup = f.read()
            db.execute(setup)
        snap = Snapshot('snap_timestamp', self.dbt_test_project_folder)
        snap.delta_calc()
        snap.delta_apply()

    def test_check_no_delta_table(self):

        with DatabaseConnection(self.dbt_test_project_folder) as db:
            with open(f"sql/{db.config['db_adapter']}/snapshot_init.sql", 'r') as f:
                setup = f.read()
            db.execute(setup)
        snap = Snapshot('snap_check_no_delta_table', self.dbt_test_project_folder)
        snap.build()

        with DatabaseConnection(self.dbt_test_project_folder) as db:
            with open(f"sql/{db.config['db_adapter']}/snapshot_incr.sql", 'r') as f:
                setup = f.read()
            db.execute(setup)
        snap = Snapshot('snap_check_no_delta_table', self.dbt_test_project_folder)
        snap.build()

    def test_check_processed(self):

        with DatabaseConnection(self.dbt_test_project_folder) as db:
            with open(f"sql/{db.config['db_adapter']}/snapshot_init.sql", 'r') as f:
                setup = f.read()
            db.execute(setup)
        snap = Snapshot('snap_check_processed', self.dbt_test_project_folder)
        snap.build()

        with DatabaseConnection(self.dbt_test_project_folder) as db:
            with open(f"sql/{db.config['db_adapter']}/snapshot_incr.sql", 'r') as f:
                setup = f.read()
            db.execute(setup)
        snap = Snapshot('snap_check_processed', self.dbt_test_project_folder)
        snap.build()

    def test_check_no_data(self):

        with DatabaseConnection(self.dbt_test_project_folder) as db:
            with open(f"sql/{db.config['db_adapter']}/snapshot_init_no_data_fields.sql", 'r') as f:
                setup = f.read()
            db.execute(setup)
        snap = Snapshot('snap_check_no_data', self.dbt_test_project_folder)
        snap.build()

        with DatabaseConnection(self.dbt_test_project_folder) as db:
            with open(f"sql/{db.config['db_adapter']}/snapshot_incr_no_data_fields.sql", 'r') as f:
                setup = f.read()
            db.execute(setup)
        snap = Snapshot('snap_check_no_data', self.dbt_test_project_folder)
        snap.build()

    def test_check_new_fields(self):

        with DatabaseConnection(self.dbt_test_project_folder) as db:
            with open(f"sql/{db.config['db_adapter']}/snapshot_init.sql", 'r') as f:
                setup = f.read()
            db.execute(setup)
        snap = Snapshot('snap_check_new_fields', self.dbt_test_project_folder)
        snap.build()

        with DatabaseConnection(self.dbt_test_project_folder) as db:
            with open(f"sql/{db.config['db_adapter']}/snapshot_incr_new_fields.sql", 'r') as f:
                setup = f.read()
            db.execute(setup)
        snap = Snapshot('snap_check_new_fields', self.dbt_test_project_folder)
        snap.build()

    def test_check_with_model(self):

        with DatabaseConnection(self.dbt_test_project_folder) as db:
            with open(f"sql/{db.config['db_adapter']}/snapshot_init.sql", 'r') as f:
                setup = f.read()
            db.execute(setup)
        snap = Snapshot('snap_check_with_model', self.dbt_test_project_folder)
        snap.build()

        with DatabaseConnection(self.dbt_test_project_folder) as db:
            with open(f"sql/{db.config['db_adapter']}/snapshot_incr.sql", 'r') as f:
                setup = f.read()
            db.execute(setup)
        snap = Snapshot('snap_check_with_model', self.dbt_test_project_folder)
        snap.build()


if __name__ == '__main__':
    main()

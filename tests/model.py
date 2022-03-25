from unittest import TestCase, main
from dbt_light.db_connection.database_connection import DatabaseConnection
from dbt_light.model import Model


class TestScd2Loader(TestCase):

    @classmethod
    def setUpClass(cls):
        dbt_test_project = 'dbt_test_project'
        cls.dbt_test_project = dbt_test_project
        cls.models = ['usual_model', 'incr_model', 'vw_model']
        with DatabaseConnection(dbt_test_project) as db:
            with open(f"sql/{db.config['adapter']}/model_init_drop.sql", 'r') as f:
                setup = f.read()
            db.execute(setup)

    def execute_test(self, models: list, mode: str):
        with DatabaseConnection(self.dbt_test_project) as db:
            with open(f"sql/{db.config['adapter']}/model_{mode}.sql", 'r') as f:
                setup = f.read()
            db.execute(setup)

        for model in models:
            mod = Model(model, self.dbt_test_project)
            mod.materialize()

    def test_snap(self):
        with self.subTest():
            self.execute_test(self.models, 'init')
        with self.subTest():
            self.execute_test(self.models, 'incr')
        # TODO: add asserts


if __name__ == '__main__':
    main()

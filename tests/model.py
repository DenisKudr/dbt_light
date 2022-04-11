from unittest import TestCase, main
from dbt_light.db_connection.database_connection import DatabaseConnection
from dbt_light.exceptions import TestsFailed
from dbt_light.model import Model


class TestModel(TestCase):

    @classmethod
    def setUpClass(cls):
        dbt_test_project = 'dbt_test_project'
        cls.dbt_test_project = dbt_test_project
        cls.models = ['usual_model', 'model_with_macro', 'incr_model', 'vw_model', 'model_with_statement']
        with DatabaseConnection(dbt_test_project) as db:
            with open(f"sql/{db.config['adapter']}/model_init_drop.sql", 'r') as f:
                setup = f.read()
            db.execute(setup)

    def execute_test(self, models: list, mode: str, full_refresh: bool = False):
        with DatabaseConnection(self.dbt_test_project) as db:
            with open(f"sql/{db.config['adapter']}/model_{mode}.sql", 'r') as f:
                setup = f.read()
            db.execute(setup)

        for model in models:
            mod = Model(model, self.dbt_test_project, full_refresh=full_refresh)
            try:
                mod.materialize()
            except TestsFailed as er:
                if mode == 'incr_failed_tests':
                    print(str(er))
                else:
                    raise Exception('Failed') from er

    def test_model(self):
        with self.subTest():
            self.execute_test(self.models, 'init')
        with self.subTest():
            self.execute_test(self.models, 'incr')
        with self.subTest():
            self.execute_test(self.models, 'init', True)
        with self.subTest():
            self.execute_test(self.models, 'incr_new_fields')
        with self.subTest():
            self.execute_test(self.models, 'incr_failed_tests')
        # TODO: add asserts


if __name__ == '__main__':
    main()

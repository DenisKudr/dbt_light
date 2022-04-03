from unittest import TestCase, main
from dbt_light.seed import Seed


class TestSeed(TestCase):

    @classmethod
    def setUpClass(cls):
        dbt_test_project = 'dbt_test_project'
        cls.dbt_test_project = dbt_test_project

    def test_seed(self):
        seed = Seed('seed', self.dbt_test_project, full_refresh=True)
        seed.materialize()

        seed = Seed('seed', self.dbt_test_project)
        seed.materialize()
        # TODO: add asserts


if __name__ == '__main__':
    main()

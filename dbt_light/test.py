from typing import Literal
from dbt_light.db_connection.database_connection import DatabaseConnection
from dbt_light.exceptions import TestsFailed


class Test:

    def __init__(self, conn: DatabaseConnection,  model: str, tests: dict):
        self.conn = conn
        self.model = model
        self.tests = tests

    def run(self, on_test_fail: Literal['error', 'error_with_rollback'], start_field: str = None) -> None:
        failed_tests = {}
        for column in self.tests.keys():
            for test in self.tests[column]:
                if type(test) == str:
                    test_name = test
                    test_args = {'model': self.model, 'column': column, 'start_field': start_field}
                    result = self.conn.execute_templated_query(f'tests/{test_name}.sql', test_args, 'query')[0][0]
                else:
                    test_name = list(test.keys())[0]
                    test_args = test[test_name]
                    test_args.update({'model': self.model, 'column': column, 'start_field': start_field})
                    result = self.conn.execute_templated_query(f'tests/{test_name}.sql', test_args, 'query')[0][0]
                if result:
                    if not failed_tests.get(column):
                        failed_tests[column] = [test_name]
                    else:
                        failed_tests[column] = failed_tests[column] + [test_name]
        if failed_tests:
            if on_test_fail == 'error_with_rollback':
                self.conn.rollback()
            raise TestsFailed(self.model, failed_tests)

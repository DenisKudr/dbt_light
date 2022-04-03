from typing import Literal
from dbt_light.context.context import Context
from dbt_light.db_connection.database_connection import DatabaseConnection
from dbt_light.exceptions import TestsFailed


class Test:

    def __init__(self, conn: DatabaseConnection, model: str, tests: dict, context: Context, start_field: str = None):
        self.conn = conn
        self.tests = tests
        self.test_context = {'schemas_context': context.schemas_context(),
                             'model': model, 'start_field': start_field}

    def run(self, on_test_fail: Literal['error', 'error_with_rollback']) -> None:
        failed_tests = {}
        for column in self.tests.keys():
            for test in self.tests[column]:
                if type(test) == str:
                    test_name = test
                    self.test_context['column'] = column
                    result = self.conn.execute_templated_query(f'tests/{test_name}.sql', self.test_context, 'query')[0][0]
                else:
                    test_name = list(test.keys())[0]
                    self.test_context['column'] = column
                    self.test_context.update(test[test_name])
                    result = self.conn.execute_templated_query(f'tests/{test_name}.sql', self.test_context, 'query')[0][0]
                if result:
                    if not failed_tests.get(column):
                        failed_tests[column] = [test_name]
                    else:
                        failed_tests[column] = failed_tests[column] + [test_name]
        if failed_tests:
            if on_test_fail == 'error_with_rollback':
                self.conn.rollback()
            raise TestsFailed(self.test_context['model'], failed_tests)

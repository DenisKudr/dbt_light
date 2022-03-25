import psycopg2
from typing import Literal, Union
from jinja2 import Environment, PackageLoader
from dbt_light.db_connection.postgres_connection import PostgresConnection
from dbt_light.exceptions import DBOperationalError, DBConnectionError
from dbt_light.helpers.config_read import config_read


class DatabaseConnection:

    def __init__(self, dbt_project: str = None):

        self.config = config_read(dbt_project, 'profiles')
        db_adapter_mapping = {'postgres': PostgresConnection}
        try:
            self.db_conn = db_adapter_mapping[self.config.get('adapter')](self.config)
        except psycopg2.Error as er:
            raise DBConnectionError from er

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.db_conn.close()

    def query(self, sql: str, params: tuple = None) -> list:
        return self.db_conn.query(sql, params)

    def execute(self, sql: str, params: tuple = None):
        self.db_conn.execute(sql, params)

    def execute_templated_query(self, template_name: str, context: dict,
                                operation_type: Literal["query", "execute"]) -> Union[list, None]:

        template_loader = PackageLoader('dbt_light', f"sql_templates/{self.config['adapter']}")
        template_env = Environment(loader=template_loader)
        template = template_env.get_template(template_name)
        query = template.render(context)
        try:
            result = getattr(self, operation_type)(query)
        except psycopg2.Error as er:
            raise DBOperationalError(query) from er
        return result

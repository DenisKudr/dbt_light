from dbt_light.context.context import Context
from dbt_light.db_connection.database_connection import DatabaseConnection
from dbt_light.db_connection.with_connection import with_connection
from dbt_light.test import Test


class Seed:

    def __init__(self, seed_name: str, dbt_project: str = None, full_refresh: bool = False):
        self.dbt_project = dbt_project
        self.full_refresh = full_refresh
        self.context = Context(dbt_project)
        self.seed_context = self.context.seed_context.get_seed(seed_name)

    @with_connection
    def materialize(self, conn: DatabaseConnection):
        if not self.full_refresh:
            target_fields = conn.execute_templated_query('get_fields.sql',
                                                         {
                                                             'schema': self.seed_context['target_schema'],
                                                             'model': self.seed_context['seed']
                                                         }, 'query')
            seed_exist = True
        else:
            target_fields = []
            seed_exist = False

        seed_fields = [(colnm.upper(), type, None) for colnm, type in list(self.seed_context['columns'].items())]
        if target_fields:
            target_fields = [column[0] for column in target_fields]
            new_fields = [input_col for input_col in seed_fields if input_col[0] not in target_fields]
            if new_fields:
                conn.execute_templated_query('add_fields.sql',
                                             {
                                                 'schema': self.seed_context['target_schema'],
                                                 'model': self.seed_context['seed'],
                                                 'new_fields_with_datatypes': new_fields
                                             }, 'execute')
        self.seed_context.update({
            'seed_fields': seed_fields,
            'seed_exist': seed_exist
        })

        conn.execute_templated_query('seed_create.sql', self.seed_context, 'execute')
        with open(self.seed_context['path'], 'r') as seed:
            conn.execute_templated_query('seed_materialize.sql', self.seed_context, 'copy', seed)
        if self.seed_context.get('tests'):
            Test(conn, f"{self.seed_context['target_schema']}.{self.seed_context['seed']}",
                 self.seed_context['tests'], self.context).run(self.seed_context['on_test_fail'])


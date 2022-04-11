from dbt_light.context.context import Context
from dbt_light.db_connection.with_connection import with_connection
from dbt_light.db_connection.database_connection import DatabaseConnection
from dbt_light.exceptions import ModelNotFound
from dbt_light.test import Test


class Model:

    def __init__(self, model_name: str, dbt_project: str = None, full_refresh: bool = False):
        self.dbt_project = dbt_project
        self.full_refresh = full_refresh
        self.context = Context(dbt_project)
        self.model_context = self.context.model_context.get_model(model_name)
        if not self.model_context:
            raise ModelNotFound(model_name)

    @with_connection
    def materialize(self, conn: DatabaseConnection) -> None:
        # check if target exists
        # for views and full_refresh consider target nonexistent by default
        if not self.full_refresh and self.model_context['materialization'] == 'table':
            target_fields = conn.execute_templated_query('get_fields.sql',
                                                         {
                                                             'schema': self.model_context['target_schema'],
                                                             'model': self.model_context['model']
                                                         }, 'query')
        else:
            target_fields = []
        if target_fields:
            model_exist = True
            target_fields = [column[0] for column in target_fields]
        else:
            model_exist = False
        # create temporary table
        temp_table_context = {
            'model_exist': model_exist,
            'this': f"{self.model_context['target_schema']}.{self.model_context['model']}"
        }

        rendered_model = self.context.render_model(self.model_context['model_sql'],
                                                   temp_table_context, conn)
        temp_table_context.update({
            'model': self.model_context['model'],
            'model_sql': rendered_model
        })
        conn.execute_templated_query('create_temp_table.sql', temp_table_context, 'execute')
        temp_schema = conn.execute_templated_query('get_temp_schema.sql', {}, 'query')[0][0]
        input_fields = conn.execute_templated_query('get_fields.sql',
                                                    {
                                                        'schema': temp_schema,
                                                        'model': self.model_context.get('model'),
                                                        'include_data_types': True
                                                    }, 'query')
        model_fields = [input_col[0] for input_col in input_fields
                        if input_col[0] != self.model_context.get('incr_key', '').upper()]
        # add new fields to target if exists
        if target_fields:
            new_fields = [input_col for input_col in input_fields if input_col[0] not in target_fields]
            if new_fields:
                conn.execute_templated_query('add_fields.sql',
                                             {
                                                 'schema': self.model_context['target_schema'],
                                                 'model': self.model_context['model'],
                                                 'new_fields_with_datatypes': new_fields
                                             }, 'execute')
        # materialize model from temp table
        self.model_context.update({
            'model_exist': model_exist,
            'this': f"{self.model_context.get('target_schema')}.{self.model_context.get('model')}",
            'model_fields': model_fields,
            'model_sql': rendered_model
        })
        conn.execute_templated_query('model_materialize.sql', self.model_context, 'execute')

        if self.model_context.get('tests'):
            Test(conn, f"{self.model_context['target_schema']}.{self.model_context['model']}",
                 self.model_context['tests'], self.context).run(self.model_context['on_test_fail'])

from dbt_light.context.context import Context
from dbt_light.db_connection.with_connection import with_connection
from dbt_light.db_connection.database_connection import DatabaseConnection


class Model:

    def __init__(self, model_name: str, dbt_project: str = None, full_refresh: bool = False):
        self.dbt_project = dbt_project
        self.context = Context(dbt_project)
        self.model_context = self.context.model_context.get_model(model_name)
        self.full_refresh = full_refresh

    @with_connection
    def materialize(self, conn: DatabaseConnection) -> None:
        model_exist = False
        model_fields = []
        if not self.full_refresh:
            model_fields = conn.execute_templated_query('model_get_fields.sql', self.model_context, 'query')
            if model_fields:
                model_exist = True
                model_fields = [column[0] for column in model_fields]
                if self.model_context.get('incr_key'):
                    model_fields.remove(self.model_context.get('incr_key').upper())

        self.model_context.update({
            'model_exist': model_exist,
            'this': f"{self.model_context.get('target_schema')}.{self.model_context.get('model')}",
            'model_fields': model_fields
        })

        self.model_context['model_sql'] = self.context.render_model(self.model_context.get('model_sql'),
                                                                    self.model_context)
        conn.execute_templated_query('model_materialize.sql', self.model_context, 'execute')

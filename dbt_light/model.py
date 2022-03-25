from dbt_light.helpers.model_render import model_render
from dbt_light.db_connection.with_connection import with_connection
from dbt_light.db_connection.database_connection import DatabaseConnection
from dbt_light.helpers.model_search import model_search


class Model:

    def __init__(self, dbt_project_folder: str, model_name: str):
        self.dbt_project_folder = dbt_project_folder
        self.context = model_search(dbt_project_folder, model_name=model_name)

    @with_connection
    def materialize(self, conn: DatabaseConnection) -> None:
        model_exist = False
        model_fields = []
        if self.context.get('is_incremental'):
            model_fields = conn.execute_templated_query('model_get_fields.sql', self.context, 'query')
            if model_fields:
                model_exist = True
                model_fields = [column[0] for column in model_fields]
                if self.context.get('seq_key'):
                    model_fields.remove(self.context.get('seq_key').upper())

        self.context.update({
            'model_exist': model_exist,
            'this': f"{self.context.get('model_schema')}.{self.context.get('model_name')}",
            'model_fields': model_fields
        })

        self.context['model'] = model_render(self.dbt_project_folder, self.context.get('model'), self.context)
        conn.execute_templated_query('model_materialize.sql', self.context, 'execute')


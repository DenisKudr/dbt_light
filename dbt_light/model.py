from dbt_light.helpers.model_render import model_render
from dbt_light.db_connection.with_connection import with_connection
from dbt_light.db_connection.database_connection import DatabaseConnection
from dbt_light.helpers.model_search import model_search


class Model:

    def __init__(self, model_name: str, dbt_project_folder: str):

        model = model_search(dbt_project_folder, model_name)
        self.dbt_project_folder = dbt_project_folder
        self.model_name = model_name
        self.schema = model['schema']
        self.path = model['path']
        self.is_incremental = model['is_incremental']
        self.materialization = model['materialization']
        self.seq_key = model['seq_key']
        self.model = model['model']

    @with_connection
    def materialize(self, conn: DatabaseConnection) -> None:
        model_exist = False
        model_fields = []
        if self.is_incremental:
            model_fields = conn.execute_templated_query('model_get_fields.sql', {
                'model_name': self.model_name,
                'model_schema': self.schema
            }, 'query')
            if model_fields:
                model_exist = True
                model_fields = [column[0] for column in model_fields]
                if self.seq_key:
                    model_fields.remove(self.seq_key.upper())

        model = model_render(self.dbt_project_folder, self.model, model_exist, f'{self.schema}.{self.model_name}')
        conn.execute_templated_query('model_materialize.sql', {'model_name': self.model_name,
                                                               'model_schema': self.schema,
                                                               'model': model,
                                                               'materialization': self.materialization,
                                                               'is_incremental': self.is_incremental,
                                                               'seq_key': self.seq_key,
                                                               'model_fields': model_fields,
                                                               'model_exist': model_exist}, 'execute')


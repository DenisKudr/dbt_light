from dbt_light.db_connection.database_connection import DatabaseConnection
from dbt_light.db_connection.with_connection import with_connection
from dbt_light.exceptions import InputTableNotFound, DeltaTableNotFound, DBOperationalError
from dbt_light.helpers.model_render import model_render
from dbt_light.helpers.model_search import model_search


class Snapshot:

    def __init__(self, dbt_project_folder: str, snapshot_name: str):
        self.dbt_project_folder = dbt_project_folder
        self.context = model_search(dbt_project_folder, snapshot_name=snapshot_name)

    def prepare_context(self, conn: DatabaseConnection) -> None:

        if self.context.get('max_dttm'):
            self.context.update({'max_dttm': self.context.get('max_dttm').strftime('%Y-%m-%d %H:%M:%S')})

        self.context.update({key: [value] for key, value in self.context.items() if
                             key in ['key_fields', 'ignored_data_fields', 'data_fields'] and type(value) == str})

        model = self.context.get('model')
        if model:
            rendered_model = model_render(self.dbt_project_folder, model=model)
            self.context['input_table'] = 'temp_' + self.context['snapshot']
            conn.execute_templated_query('model_materialize.sql', {'model': rendered_model,
                                                                   'model_name': self.context['input_table'],
                                                                   'materialization': 'temp_table'
                                                                   }, 'execute')
            self.context['source_schema'] = conn.execute_templated_query('snapshot_get_temp_schema.sql',
                                                                         {}, 'query')[0][0]

        input_fields = conn.execute_templated_query('model_get_fields.sql',
                                                    {'model_schema': self.context['source_schema'],
                                                     'model_name': self.context['input_table'],
                                                     'include_data_types': True}, 'query')
        if len(input_fields) == 0:
            raise InputTableNotFound(f"{self.context.get('source_schema')}.{self.context.get('input_table')}")

        snapshot_fields = conn.execute_templated_query('model_get_fields.sql',
                                                       {'model_schema': self.context['target_schema'],
                                                        'model_name': self.context['snapshot']},
                                                       'query')
        init_load = True if len(snapshot_fields) == 0 else False

        all_data_fields = [column[0] for column in input_fields
                           if column[0] not in list(map(lambda x: x.upper(),
                                                        self.context.get('key_fields') + [
                                                            self.context.get('updated_at_field'),
                                                            self.context.get('processed_field'),
                                                            self.context.get('hash_diff_field')]))]
        data_fields = self.context.get('data_fields')
        if not data_fields:
            data_fields = all_data_fields
        if self.context.get('ignored_data_fields'):
            data_fields = [column for column in data_fields
                           if column not in list(map(lambda x: x.upper(), self.context.get('ignored_data_fields')))]

        processed_field_exist = True if self.context.get('processed_field') in input_fields else False
        hash_diff_field_exist = True if self.context.get('hash_diff_field') in input_fields else False

        new_fields, new_fields_with_datatypes = [], []
        if not init_load:
            new_fields = [input_col for input_col in all_data_fields if input_col not in
                          [snap_col[0] for snap_col in snapshot_fields]]
            if new_fields:
                all_data_fields = [column for column in all_data_fields if column not in new_fields]
                new_fields_with_datatypes = [column for column in input_fields if column[0] in new_fields]

        self.context.update({
            'all_data_fields': all_data_fields,
            'data_fields': data_fields,
            'processed_field_exist': processed_field_exist,
            'hash_diff_field_exist': hash_diff_field_exist,
            'init_load': init_load,
            'new_fields': new_fields,
            'new_fields_with_datatypes': new_fields_with_datatypes
        })

    @with_connection
    def delta_calc(self, conn: DatabaseConnection = None) -> None:
        self.prepare_context(conn)
        conn.execute_templated_query('snapshot_delta_calc.sql', self.context, 'execute')

    @with_connection
    def delta_apply(self, conn: DatabaseConnection = None) -> None:
        if self.context.get('init_load'):
            conn.execute_templated_query('snapshot_create.sql', self.context, 'execute')
        if self.context.get('new_fields'):
            conn.execute_templated_query('snapshot_add_fields.sql', self.context, 'execute')
        try:
            statistics = conn.execute_templated_query('snapshot_delta_count.sql', self.context, 'query')
        except DBOperationalError as er:
            raise DeltaTableNotFound(f"{self.context.get('delta_schema')}.{self.context.get('delta_table')}") from er
        new_objects, new_values, deleted_objects, direct_upd_objects = statistics[0][0], statistics[0][1], \
                                                                       statistics[0][2], statistics[0][3]
        if new_values or deleted_objects or direct_upd_objects:
            conn.execute_templated_query('snapshot_update.sql', self.context, 'execute')
        if new_objects or new_values:
            conn.execute_templated_query('snapshot_insert.sql', self.context, 'execute')

    @with_connection
    def build(self, conn: DatabaseConnection = None) -> None:
        self.delta_calc(conn=conn)
        self.delta_apply(conn=conn)


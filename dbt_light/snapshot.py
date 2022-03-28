from dbt_light.context.context import Context
from dbt_light.db_connection.database_connection import DatabaseConnection
from dbt_light.db_connection.with_connection import with_connection
from dbt_light.exceptions import InputTableNotFound, DeltaTableNotFound, DBOperationalError


class Snapshot:

    def __init__(self, snapshot_name: str, dbt_project: str = None):
        self.dbt_project = dbt_project
        self.context = Context(dbt_project)
        self.snapshot_context = self.context.snapshot_context.get_snapshot(snapshot_name)

    def prepare_context(self, conn: DatabaseConnection) -> None:

        model = self.snapshot_context.get('model_sql')
        if model:
            rendered_model = self.context.render_model(model)
            conn.execute_templated_query('model_materialize.sql', {'model_sql': rendered_model,
                                                                   'model': self.snapshot_context['input_table'],
                                                                   'materialization': 'temp_table'
                                                                   }, 'execute')
            self.snapshot_context['source_schema'] = conn.execute_templated_query('snapshot_get_temp_schema.sql',
                                                                                  {}, 'query')[0][0]

        input_fields = conn.execute_templated_query('model_get_fields.sql',
                                                    {'target_schema': self.snapshot_context['source_schema'],
                                                     'model': self.snapshot_context['input_table'],
                                                     'include_data_types': True}, 'query')
        if len(input_fields) == 0:
            raise InputTableNotFound(f"{self.snapshot_context.get('source_schema')}.{self.snapshot_context.get('input_table')}")

        snapshot_fields = conn.execute_templated_query('model_get_fields.sql',
                                                       {'target_schema': self.snapshot_context['target_schema'],
                                                        'model': self.snapshot_context['snapshot']},
                                                       'query')
        init_load = True if len(snapshot_fields) == 0 else False

        all_data_fields = [column[0] for column in input_fields
                           if column[0] not in list(map(lambda x: x.upper(),
                                                        self.snapshot_context.get('key_fields') + [
                                                            self.snapshot_context.get('updated_at_field'),
                                                            self.snapshot_context.get('processed_field'),
                                                            self.snapshot_context.get('hash_diff_field')]))]
        data_fields = self.snapshot_context.get('data_fields')
        if not data_fields:
            data_fields = all_data_fields
        if self.snapshot_context.get('ignored_data_fields'):
            data_fields = [column for column in data_fields
                           if column not in list(map(lambda x: x.upper(), self.snapshot_context.get('ignored_data_fields')))]

        processed_field_exist = True if self.snapshot_context.get('processed_field') in input_fields else False
        hash_diff_field_exist = True if self.snapshot_context.get('hash_diff_field') in input_fields else False

        new_fields, new_fields_with_datatypes = [], []
        if not init_load:
            new_fields = [input_col for input_col in all_data_fields if input_col not in
                          [snap_col[0] for snap_col in snapshot_fields]]
            if new_fields:
                all_data_fields = [column for column in all_data_fields if column not in new_fields]
                new_fields_with_datatypes = [column for column in input_fields if column[0] in new_fields]

        self.snapshot_context.update({
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
        conn.execute_templated_query('snapshot_delta_calc.sql', self.snapshot_context, 'execute')

    @with_connection
    def delta_apply(self, conn: DatabaseConnection = None) -> None:
        if self.snapshot_context.get('init_load'):
            conn.execute_templated_query('snapshot_create.sql', self.snapshot_context, 'execute')
        if self.snapshot_context.get('new_fields'):
            conn.execute_templated_query('snapshot_add_fields.sql', self.snapshot_context, 'execute')
        try:
            statistics = conn.execute_templated_query('snapshot_delta_count.sql', self.snapshot_context, 'query')
        except DBOperationalError as er:
            raise DeltaTableNotFound(f"{self.snapshot_context.get('delta_schema')}.{self.snapshot_context.get('delta_table')}") from er
        new_objects, new_values, deleted_objects, direct_upd_objects = statistics[0][0], statistics[0][1], \
                                                                       statistics[0][2], statistics[0][3]
        if new_values or deleted_objects or direct_upd_objects:
            conn.execute_templated_query('snapshot_update.sql', self.snapshot_context, 'execute')
        if new_objects or new_values:
            conn.execute_templated_query('snapshot_insert.sql', self.snapshot_context, 'execute')

    @with_connection
    def materialize(self, conn: DatabaseConnection = None) -> None:
        self.delta_calc(conn=conn)
        self.delta_apply(conn=conn)


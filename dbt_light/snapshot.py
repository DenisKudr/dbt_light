from dbt_light.context.context import Context
from dbt_light.db_connection.database_connection import DatabaseConnection
from dbt_light.db_connection.with_connection import with_connection
from dbt_light.exceptions import InputTableNotFound, DeltaTableNotFound, DBOperationalError, SnapshotNotFound, \
    NoInputSpecifiedError
from dbt_light.test import Test


class Snapshot:

    def __init__(self, snapshot_name: str, dbt_project: str = None, full_refresh: bool = False):
        self.dbt_project = dbt_project
        self.full_refresh = full_refresh
        self.context = Context(dbt_project)
        self.snapshot_context = self.context.snapshot_context.get_snapshot(snapshot_name)
        if not self.snapshot_context:
            raise SnapshotNotFound(snapshot_name)

    def prepare_context(self, conn: DatabaseConnection) -> None:
        model = self.snapshot_context.get('model_sql')
        if model:
            rendered_model = self.context.render_model(model, {})
            conn.execute_templated_query('create_temp_table.sql', {'model_sql': rendered_model,
                                                                   'model': self.snapshot_context['input_table']
                                                                   }, 'execute')
            self.snapshot_context['source_schema'] = conn.execute_templated_query('get_temp_schema.sql',
                                                                                  {}, 'query')[0][0]
        else:
            schemas_context = self.context.schemas_context()
            input_table = self.snapshot_context['model']
            if len(input_table.split('.')) > 1:
                source = input_table.split('.')[0]
                input_table = input_table.split('.')[1]
                schema_table = schemas_context.get(source).get(input_table)
                if not schema_table:
                    raise NoInputSpecifiedError(input_table)
                self.snapshot_context['input_table'] = schema_table.split('.')[1]
                self.snapshot_context['source_schema'] = schema_table.split('.')[0]
            else:
                schema = schemas_context.get(input_table)
                if not schema:
                    raise NoInputSpecifiedError(input_table)
                self.snapshot_context['input_table'] = input_table
                self.snapshot_context['source_schema'] = schema

        input_fields = conn.execute_templated_query('get_fields.sql',
                                                    {'schema': self.snapshot_context['source_schema'],
                                                     'model': self.snapshot_context['input_table'],
                                                     'include_data_types': True}, 'query')
        if len(input_fields) == 0:
            raise InputTableNotFound(f"{self.snapshot_context.get('source_schema')}.{self.snapshot_context.get('input_table')}")

        if not self.full_refresh:
            snapshot_fields = conn.execute_templated_query('get_fields.sql',
                                                           {'schema': self.snapshot_context['target_schema'],
                                                            'model': self.snapshot_context['snapshot']},
                                                           'query')
            init_load = True if len(snapshot_fields) == 0 else False
        else:
            snapshot_fields = None
            init_load = True

        all_data_fields = [column[0] for column in input_fields
                           if column[0] not in list(map(lambda x: x.upper(),
                                                        self.snapshot_context.get('key_fields') + [
                                                            self.snapshot_context.get('updated_at_field'),
                                                            self.snapshot_context.get('processed_field'),
                                                            self.snapshot_context.get('hash_diff_field')]))]
        if self.snapshot_context.get('deleted_flg'):
            all_data_fields = [field for field in all_data_fields if field not in self.snapshot_context['deleted_flg'].upper()]
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
        if self.snapshot_context.get('new_fields') and self.snapshot_context.get('delta_table') != 'temp_delta_table':
            conn.execute_templated_query('add_fields.sql', {
                'new_fields_with_datatypes': self.snapshot_context.get('new_fields_with_datatypes'),
                'schema': self.snapshot_context.get('delta_schema'),
                'model': self.snapshot_context.get('delta_table')
            }, 'execute')
        conn.execute_templated_query('snapshot_delta_calc.sql', self.snapshot_context, 'execute')

    @with_connection
    def delta_apply(self, conn: DatabaseConnection = None) -> None:
        if self.snapshot_context.get('init_load'):
            conn.execute_templated_query('snapshot_create.sql', self.snapshot_context, 'execute')
        if self.snapshot_context.get('new_fields'):
            conn.execute_templated_query('add_fields.sql', {
                'new_fields_with_datatypes': self.snapshot_context.get('new_fields_with_datatypes'),
                'schema': self.snapshot_context.get('target_schema'),
                'model': self.snapshot_context.get('snapshot')
            }, 'execute')
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
        if self.snapshot_context.get('updated_at_field'):
            conn.execute_templated_query('snapshot_update_multiple_versions.sql', self.snapshot_context, 'execute')

        if self.snapshot_context.get('tests'):
            Test(conn, f"{self.snapshot_context['target_schema']}.{self.snapshot_context['snapshot']}",
                 self.snapshot_context['tests'], self.context,
                 self.snapshot_context['start_field']).run(self.snapshot_context['on_test_fail'])

    @with_connection
    def materialize(self, conn: DatabaseConnection = None) -> None:
        self.delta_calc(conn=conn)
        self.delta_apply(conn=conn)

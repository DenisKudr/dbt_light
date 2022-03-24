from dbt_light.db_connection.database_connection import DatabaseConnection


def with_connection(func):
    def wrapper(*args, **kwargs):
        if kwargs.get('conn'):
            return func(*args, **kwargs)
        else:
            dbt_project_folder = args[0].dbt_project_folder
            with DatabaseConnection(dbt_project_folder) as db:
                kwargs.update({'conn': db})
                return func(*args, **kwargs)
    return wrapper

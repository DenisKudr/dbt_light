from dbt_light.db_connection.database_connection import DatabaseConnection


def with_connection(func):
    def wrapper(*args, **kwargs):
        if kwargs.get('conn'):
            return func(*args, **kwargs)
        else:
            dbt_project = args[0].dbt_project
            with DatabaseConnection(dbt_project) as db:
                kwargs.update({'conn': db})
                return func(*args, **kwargs)
    return wrapper

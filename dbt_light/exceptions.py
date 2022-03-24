class DbtLightError(Exception):
    """ Base class for all dbt_light errors """


class SnapshotNotFound(DbtLightError):
    """" Raise if there is no snapshot with specified name in config """

    def __init__(self, snapshot, snapshots_config_path):
        super().__init__(f'No snapshot named {snapshot} in {snapshots_config_path}')


class ModelNotFound(DbtLightError):
    """" Raise if there is no model with specified name in project folder """

    def __init__(self, model):
        super().__init__(f'No model named {model}')


class DuplicateSnapshotsError(DbtLightError):
    """" Raise if there are multiple snapshots with the same name in config """

    def __init__(self, snapshot, snapshots_config_path):
        super().__init__(f'There are more than one snapshot defined with the name '
                         f'{snapshot} in {snapshots_config_path}')


class DBTProjectNotFound(DbtLightError):
    """" Raise if dbt project cannot be located """

    def __init__(self, dbt_project_folder):
        super().__init__(f"Can't access dbt project folder: {dbt_project_folder}")


class ConfigReadError(DbtLightError):
    """" Raise if reading config in dbt project failed """

    def __init__(self, config):
        super().__init__(f"Can't read {config}")


class ConfigValidateError(DbtLightError):
    """" Raise if config is invalid """

    def __init__(self, config):
        super().__init__(f"Can't validate {config}")


class InputTableNotFound(DbtLightError):
    """ Raise if input table doesn't exist when building snapshot """

    def __init__(self, table):
        super().__init__(f"Input table {table} doesn't exist")


class NoInputSpecifiedError(DbtLightError):
    """ Raise if input table doesn't exist when building snapshot """

    def __init__(self, snapshot):
        super().__init__(f"No input specified for {snapshot}")


class DeltaTableNotFound(DbtLightError):
    """ Raise if delta table doesn't exist when applying delta """

    def __init__(self, table):
        super().__init__(f"Delta table {table} doesn't exist")


class DBOperationalError(DbtLightError):
    """ Raise if error occurs when executing query """

    def __init__(self, query):
        super().__init__(f"Got error when executing query:\n{query}")


class DBConnectionError(DbtLightError):
    """ Raise if error occurs when connecting to DB """

    def __init__(self, msg: str = "Can't connect to DB"):
        super().__init__(msg)


class DuplicateModelsError(DbtLightError):
    """" Raise if there are multiple models with the same name """

    def __init__(self, model, schemas):
        super().__init__(f'There are more than one model defined with the name '
                         f'{model} in schemas: {schemas}')


class ModelReadError(DbtLightError):
    """" Raise if reading model failed """

    def __init__(self, model):
        super().__init__(f"Can't read model {model}")


class ModelRenderError(DbtLightError):
    """" Raise if rendering model failed """

    def __init__(self, model):
        super().__init__(f"Can't render model {model}")

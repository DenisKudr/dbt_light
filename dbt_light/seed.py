from dbt_light.helpers.model_search import model_search


class Seed():
    def __init__(self, seed_name: str, dbt_project: str = None):
        self.dbt_project = dbt_project
        self.context = model_search(dbt_project, seed_name=model_name)
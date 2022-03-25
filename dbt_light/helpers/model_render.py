from jinja2 import TemplateError, TemplateSyntaxError
from jinja2.nativetypes import NativeEnvironment
from dbt_light.exceptions import ModelRenderError
from dbt_light.helpers.model_search import model_search


def model_render(model: str, cont: dict = None, dbt_project: str = None):

    context = model_search(dbt_project)
    template = NativeEnvironment().from_string(model)
    if cont:
        context.update(cont)
    try:
        rendered = template.render(context)
    except (TemplateError, TemplateSyntaxError) as er:
        raise ModelRenderError(model) from er

    return rendered


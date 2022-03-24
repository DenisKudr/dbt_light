from jinja2 import TemplateError, TemplateSyntaxError
from jinja2.nativetypes import NativeEnvironment
from dbt_light.exceptions import ModelRenderError
from dbt_light.helpers.model_search import model_search


def model_render(dbt_project_folder: str, model: str, model_exist: bool = False, this: str = None):

    context = model_search(dbt_project_folder)
    template = NativeEnvironment().from_string(model)
    context.update(
        {
            'model_exist': model_exist,
            'this': this
        }
    )
    try:
        rendered = template.render(context)
    except (TemplateError, TemplateSyntaxError) as er:
        raise ModelRenderError(model) from er

    return rendered


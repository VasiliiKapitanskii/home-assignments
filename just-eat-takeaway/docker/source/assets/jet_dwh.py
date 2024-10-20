from pathlib import Path

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

project_dir = (
    Path(__file__)
    .joinpath(
        "..",
        "..",
        "..",
        'dbt',
    )
    .resolve()
)
packaged_project_dir = Path(__file__).joinpath("..", "..", 'dbt', "dbt-project").resolve()
jet_dbt_project = DbtProject(
    project_dir=project_dir,
    packaged_project_dir=packaged_project_dir,
)
jet_dbt_project.prepare_if_dev()


@dbt_assets(manifest=jet_dbt_project.manifest_path)
def jet_dbt_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

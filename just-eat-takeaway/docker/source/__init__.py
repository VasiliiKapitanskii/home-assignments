"""Definitions that provide Dagster code locations."""
from dagster import Definitions, EnvVar
from dagster_aws.s3 import S3Resource
from dagster_dbt import DbtCliResource
from source import assets
from source.assets.jet_dwh import jet_dbt_dbt_assets, project_dir
from source.jobs import init_databases, init_tables
from source.resources.clickhouse import ClickHouseResource
from source.sensors.s3 import jet_metadata_s3_sensor, jet_reviews_s3_sensor

defs = Definitions(
    assets=[
        assets.amazon.raw_amazon_reviews,
        assets.amazon.raw_amazon_metadata,
        jet_dbt_dbt_assets,
    ],
    sensors=[
        jet_reviews_s3_sensor,
        jet_metadata_s3_sensor,
    ],
    jobs=[
        init_databases,
        init_tables,
        assets.amazon.load_raw_amazon_reviews,
        assets.amazon.load_raw_amazon_metadata,
    ],
    schedules=[
        # add schedules here
        # every_weekday_1am,
        # ScheduleDefinition(job=job, cron_schedule="@daily")
    ],
    resources={
        "s3": S3Resource(
            aws_access_key_id=EnvVar("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=EnvVar("AWS_SECRET_ACCESS_KEY"),
            region_name=EnvVar("AWS_REGION"),
        ),
        "clickhouse": ClickHouseResource(host="clickhouse", port=8123),
        "dbt": DbtCliResource(project_dir=project_dir),
    },
)

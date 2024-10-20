import os

from dagster import (
    AssetSelection,
    Config,
    DynamicPartitionsDefinition,
    OpExecutionContext,
    asset,
    define_asset_job,
)
from source.resources.clickhouse import ClickHouseResource

raw_amazon_reviews_files = DynamicPartitionsDefinition(name="raw_amazon_reviews_files")
raw_amazon_metadata_files = DynamicPartitionsDefinition(name="raw_amazon_metadata_files")


class RawDataAssetLoadConfig(Config):
    asset_config: str


AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET = os.getenv("AWS_BUCKET")
AWS_REGION = os.getenv("AWS_REGION")


@asset(partitions_def=raw_amazon_reviews_files, compute_kind="python")
def raw_amazon_reviews(
    context: OpExecutionContext,
    config: RawDataAssetLoadConfig,
    clickhouse: ClickHouseResource,
):
    _insert_raw_data(context, config, clickhouse, 'raw_thirdparty.amazon_review')


@asset(partitions_def=raw_amazon_metadata_files, compute_kind="python")
def raw_amazon_metadata(
    context: OpExecutionContext,
    config: RawDataAssetLoadConfig,
    clickhouse: ClickHouseResource,
):
    _insert_raw_data(context, config, clickhouse, 'raw_thirdparty.amazon_metadata')


def _insert_raw_data(
    context: OpExecutionContext,
    config: RawDataAssetLoadConfig,
    clickhouse: ClickHouseResource,
    table_fqn: str,
):
    """Raw data loaded from s3 to the ClickHouse tables"""
    s3_file_to_process = context.partition_key
    s3_url = f"https://{AWS_BUCKET}.s3.{AWS_REGION}.amazonaws.com/{s3_file_to_process}"

    query = f"""
        INSERT INTO {table_fqn}
        SELECT
            '{context.run.run_id}' AS _meta_run_id
            , '{s3_url}' AS _meta_loaded_from
            , now() AS _meta_loaded_at
            , csv.*
        FROM s3(
            '{s3_url}',
            '{AWS_ACCESS_KEY_ID}', '{AWS_SECRET_ACCESS_KEY}',
            'CSVWithNames'
        ) AS csv
        SETTINGS
            input_format_with_names_use_header = 1,
            input_format_allow_errors_num = 50,
            input_format_allow_errors_ratio = 5,
            max_download_threads = 12,
            max_insert_threads = 8;
    """

    other_parameters = config.asset_config
    context.log.info(
        f"Attempting to load {s3_file_to_process}. Other potential config is: {other_parameters}"
    )

    clickhouse.execute_query(query)
    context.add_output_metadata(
        metadata={
            "clickhouse_destination": table_fqn,
            "loaded_from": s3_url,
            "owner": "data@example.com",
        }
    )


load_raw_amazon_reviews = define_asset_job(
    name="load_raw_amazon_reviews",
    selection=AssetSelection.assets(raw_amazon_reviews),
    partitions_def=raw_amazon_reviews_files,
    description="loads raw amazon reviews data from s3 to clickhouse",
    tags={"owner": "data team"},
)

load_raw_amazon_metadata = define_asset_job(
    name="load_raw_amazon_metadata",
    selection=AssetSelection.assets(raw_amazon_metadata),
    partitions_def=raw_amazon_metadata_files,
    description="loads raw amazon metadata from s3 to clickhouse",
    tags={"owner": "data team"},
)

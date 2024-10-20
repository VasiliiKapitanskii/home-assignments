import os
from datetime import datetime

from dagster import RunConfig, RunRequest, SensorResult, SkipReason, sensor
from dagster_aws.s3 import S3Resource
from dagster_aws.s3.sensor import get_s3_keys
from source.assets.amazon import (
    RawDataAssetLoadConfig,
    load_raw_amazon_metadata,
    load_raw_amazon_reviews,
    raw_amazon_metadata_files,
    raw_amazon_reviews_files,
)


@sensor(job=load_raw_amazon_reviews)
def jet_reviews_s3_sensor(context, s3: S3Resource):
    return _s3_sensor_base(
        context, s3, "jet_reviews", "raw_amazon_reviews", raw_amazon_reviews_files
    )


@sensor(job=load_raw_amazon_metadata)
def jet_metadata_s3_sensor(context, s3: S3Resource):
    return _s3_sensor_base(
        context, s3, "jet_metadata", "raw_amazon_metadata", raw_amazon_metadata_files
    )


def _s3_sensor_base(context, s3: S3Resource, s3_prefix, config_name, raw_files_partition):
    """Watches for files to land in s3 and loads them to ClickHouse"""
    s3_bucket = os.getenv("AWS_BUCKET")
    since_key = context.cursor or None
    s3_client = s3.get_client()
    new_s3_keys = get_s3_keys(
        s3_bucket, since_key=since_key, prefix=s3_prefix, s3_session=s3_client
    )

    if f"{s3_prefix}/" in new_s3_keys:
        new_s3_keys.remove(f"{s3_prefix}/")

    if not new_s3_keys:
        return SkipReason("No new s3 files found for bucket.")

    last_key = new_s3_keys[-1]

    # pass additional run time parameters as asset configuration
    additional_config = RunConfig(
        {
            config_name: RawDataAssetLoadConfig(
                asset_config=f" ran from sensor config at {datetime.now()}"
            )
        }
    )

    return SensorResult(
        cursor=last_key,
        dynamic_partitions_requests=[raw_files_partition.build_add_request(new_s3_keys)],
        run_requests=[
            RunRequest(partition_key=key, run_config=additional_config) for key in new_s3_keys
        ],
    )

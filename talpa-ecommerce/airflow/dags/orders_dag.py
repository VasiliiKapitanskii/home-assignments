from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils import timezone
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor

from global_const import (
    SNOWFLAKE_CONNECTION_ID,
    S3_RAW_DATA_BUCKET,
    SNOWFLAKE_S3_STAGE
)

S3_KEY = 'sample.csv'

with DAG(
    dag_id='extract_orders',
    description='DAG to extract CSVs from S3 to Snowflake',
    start_date=datetime(2023, 8, 2, tzinfo=timezone.utc),
    catchup=False,
    schedule_interval=timedelta(hours=1),
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2023, 8, 9),
        'retries': 3
    },
    tags=['extract', 'S3', 'orders', 'team_name'],
) as dag:
    s3_sensor = S3KeySensor(
        task_id='s3_sensor',
        poke_interval=60,
        timeout=180,
        soft_fail=True,
        bucket_key=S3_KEY,
        bucket_name=S3_RAW_DATA_BUCKET
    )

    s3_to_snowflake = S3ToSnowflakeOperator(
        task_id='s3_to_snowflake',
        snowflake_conn_id=SNOWFLAKE_CONNECTION_ID,
        s3_keys=[S3_KEY],
        schema='STAGING',
        table='ORDERS',
        stage=SNOWFLAKE_S3_STAGE,
        file_format="(TYPE='CSV', SKIP_HEADER=1, FIELD_OPTIONALLY_ENCLOSED_BY='\"'), FORCE=TRUE"
    )

    # Move processed file if necessary
    # move_processed = S3FileTransformOperator(
    #     task_id='move_processed',
    #     source_s3_key=f'{SNOWFLAKE_S3_STAGE}/{S3_KEY}',
    #     dest_s3_key=f'{SNOWFLAKE_S3_STAGE}/processed/{{ ds }}/{S3_KEY}',
    #     transform_script='/bin/mv'
    # )

    s3_sensor >> s3_to_snowflake # >> move_processed

from dagster import job
from source.ops.db_setup import (
    drop_raw_metadata_op,
    drop_raw_reviews_op,
    init_dev_dwh_op,
    init_raw_metadata_op,
    init_raw_reviews_op,
    init_raw_thirdparty_dwh_op,
)


@job
def init_databases():
    init_raw_thirdparty_dwh_op()
    init_dev_dwh_op()


@job(tags={"dagster/max_retries": 1})
def init_tables():
    init_raw_metadata_op(drop_raw_metadata_op())
    init_raw_reviews_op(drop_raw_reviews_op())

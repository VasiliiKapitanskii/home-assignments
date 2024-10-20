from clickhouse_driver import Client
from dagster import op


# the initialization could be done via Terraform
@op
def init_raw_thirdparty_dwh_op():
    client = Client("clickhouse")
    client.execute("CREATE DATABASE IF NOT EXISTS raw_thirdparty;")


@op
def init_dev_dwh_op():
    client = Client("clickhouse")
    client.execute("CREATE DATABASE IF NOT EXISTS dwh_dev;")


@op
def drop_raw_metadata_op():
    client = Client("clickhouse")
    client.execute("DROP TABLE IF EXISTS raw_thirdparty.amazon_metadata;")


@op
def drop_raw_reviews_op():
    client = Client("clickhouse")
    client.execute("DROP TABLE IF EXISTS raw_thirdparty.amazon_review;")


@op
def init_raw_metadata_op(arg):
    client = Client("clickhouse")
    client.execute(
        """
        CREATE TABLE raw_thirdparty.amazon_metadata (
            _meta_run_id String,
            _meta_loaded_from String,
            _meta_loaded_at DateTime,
            metadataid String,
            asin String,
            salesrank String,
            imurl String,
            categories String,
            title String,
            description String,
            price String,
            related String,
            brand String
        ) ENGINE = MergeTree()
        ORDER BY metadataid;
    """
    )


@op
def init_raw_reviews_op(arg):
    client = Client("clickhouse")
    client.execute(
        """
        CREATE TABLE raw_thirdparty.amazon_review (
            _meta_run_id String,
            _meta_loaded_from String,
            _meta_loaded_at DateTime,
            reviewID String,
            reviewerID String,
            asin String,
            reviewerName String,
            helpful String,
            reviewText String,
            overall String,
            summary String,
            unixReviewTime String,
            reviewTime String
        ) ENGINE = MergeTree()
        ORDER BY reviewID;
    """
    )

{{
    config(
        engine='MergeTree',
        incremental_strategy='delete+insert',
    )
}}


WITH raw_data AS (
    SELECT
        _meta_run_id
        , _meta_loaded_from
        , _meta_loaded_at
        , reviewerID AS reviewer_id
        , asin AS product_id
        , reviewerName AS reviewer_name
        , replaceAll(replaceAll(helpful, '[', ''), ']', '') AS helpful_temp
        , helpful
        , reviewText AS review_text
        , replaceAll(overall, '.0', '') AS overall_rating
        , summary
        , unixReviewTime AS unix_review_time
        , reviewTime AS review_time
    FROM
        {{ source("raw_thirdparty_amazon", "amazon_review") }}
    {% if is_incremental() %}
        WHERE _meta_loaded_at > (SELECT MAX(_meta_loaded_at) FROM {{ this }})
    {% endif %}
)

, parsed AS (
    SELECT
        _meta_run_id
        , _meta_loaded_from
        , _meta_loaded_at
        , {{ dbt_utils.generate_surrogate_key(['reviewer_id', 'product_id', 'unix_review_time']) }} AS surrogate_key
        , reviewer_id
        , product_id
        , reviewer_name
        , splitByString(', ', helpful_temp) AS helpful
        , toInt32(helpful[1]) AS upvotes
        , toInt32(helpful[2]) AS downvotes
        , review_text
        , toInt32(overall_rating) AS overall_rating
        , summary
        , toInt32(unix_review_time) AS unix_review_time
        , toDate(COALESCE(
            parseDateTimeOrNull(review_time, '%m %d, %Y')
            , parseDateTimeOrNull(review_time, '%m%e, %Y')
        )) AS review_date
    FROM raw_data
)

-- dbt_utils.deduplicate is not production ready for Clickhouse?
, deduplicated AS (
    SELECT
        *
        , ROW_NUMBER() OVER (PARTITION BY surrogate_key ORDER BY _meta_loaded_at DESC) AS row_number
    FROM parsed
    QUALIFY row_number = 1
)

SELECT * EXCEPT (helpful) FROM deduplicated

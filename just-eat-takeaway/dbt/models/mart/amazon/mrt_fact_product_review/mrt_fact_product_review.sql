{{
    config(
        engine='MergeTree',
        incremental_strategy='delete+insert',
    )
}}


WITH base AS (
    SELECT
        _meta_loaded_at
        , surrogate_key
        , reviewer_id
        , product_id
        , upvotes
        , downvotes
        , review_text
        , overall_rating
        , summary
        , unix_review_time
        , review_date
    FROM {{ ref('stg_thirdparty_amazon_review') }}
    {% if is_incremental() %}
        WHERE _meta_loaded_at > (SELECT MAX(_meta_loaded_at) FROM {{ this }})
    {% endif %}
)

SELECT * FROM base

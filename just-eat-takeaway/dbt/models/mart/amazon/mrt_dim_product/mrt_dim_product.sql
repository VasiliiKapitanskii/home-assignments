{{
    config(
        engine='MergeTree',
        incremental_strategy='delete+insert',
    )
}}


WITH base AS (
    SELECT
        _meta_loaded_at
        , product_id
        , sales_rank
        , image_url
        , category_ids -- to avoid M2M relationship for simplicity
        , title
        , price
        , related
        , brand_id
    FROM {{ ref('stg_thirdparty_amazon_metadata') }}
    {% if is_incremental() %}
        WHERE _meta_loaded_at > (SELECT MAX(_meta_loaded_at) FROM {{ this }})
    {% endif %}
)

, deduplicated AS (
    SELECT
        *
        , ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY _meta_loaded_at DESC) AS row_number
    FROM base
    QUALIFY row_number = 1
)

SELECT * EXCEPT (row_number) FROM deduplicated

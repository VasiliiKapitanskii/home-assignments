{{
    config(
        engine='MergeTree',
        incremental_strategy='delete+insert',
    )
}}


WITH base AS (
    SELECT
        _meta_loaded_at
        , assumeNotNull(brand_id) AS brand_id
        , assumeNotNull(brand_name) AS brand_name
    FROM {{ ref('stg_thirdparty_amazon_metadata') }}
    WHERE brand_name IS NOT NULL
    {% if is_incremental() %}
        AND _meta_loaded_at > (SELECT MAX(_meta_loaded_at) FROM {{ this }})
    {% endif %}
)

, deduplicated AS (
    SELECT
        *
        , ROW_NUMBER() OVER (PARTITION BY brand_id ORDER BY _meta_loaded_at DESC) AS row_number
    FROM base
    QUALIFY row_number = 1
)

SELECT * EXCEPT (row_number) FROM deduplicated

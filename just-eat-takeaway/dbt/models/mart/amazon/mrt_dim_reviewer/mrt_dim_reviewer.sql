{{
    config(
        engine='MergeTree',
        incremental_strategy='delete+insert',
    )
}}


WITH base AS (
    SELECT
        _meta_loaded_at
        , reviewer_id
        , reviewer_name
    FROM {{ ref('stg_thirdparty_amazon_review') }}
    {% if is_incremental() %}
        WHERE _meta_loaded_at > (SELECT MAX(_meta_loaded_at) FROM {{ this }})
    {% endif %}
)

, deduplicated AS (
    SELECT
        *
        , ROW_NUMBER() OVER (PARTITION BY reviewer_id ORDER BY _meta_loaded_at DESC) AS row_number
    FROM base
    QUALIFY row_number = 1
)

SELECT * EXCEPT (row_number) FROM deduplicated

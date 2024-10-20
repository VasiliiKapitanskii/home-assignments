{{
    config(
        engine='MergeTree',
        incremental_strategy='delete+insert',
    )
}}


WITH base AS (
    SELECT
        _meta_loaded_at
        , category_ids AS category_id
        , category_names AS category_name
    FROM {{ ref('stg_thirdparty_amazon_metadata') }}
    ARRAY JOIN category_ids, category_names
    {% if is_incremental() %}
        WHERE _meta_loaded_at > (SELECT MAX(_meta_loaded_at) FROM {{ this }})
    {% endif %}
)

, deduplicated AS (
    SELECT
        *
        , ROW_NUMBER() OVER (PARTITION BY category_id ORDER BY _meta_loaded_at DESC) AS row_number
    FROM base
    QUALIFY row_number = 1
)

SELECT * EXCEPT (row_number) FROM deduplicated

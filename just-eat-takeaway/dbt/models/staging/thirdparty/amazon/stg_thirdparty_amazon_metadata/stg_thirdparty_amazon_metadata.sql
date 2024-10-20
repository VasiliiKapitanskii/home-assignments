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
        , metadataid AS metadata_id
        , asin AS product_id
        , salesrank AS sales_rank -- TODO: Investigate Clickhouse's experimental JSON type
        , imurl AS image_url
        , replaceAll(replaceAll(replaceAll(categories, '[[', ''), ']]', ''), '\"', '\'') AS categories_temp
        , splitByString('], [', categories_temp) AS category_groups
        , arrayMap(x -> arrayMap(y -> replaceAll(y, '\'', ''), splitByString('\', \'', x)), category_groups) AS categories_splitted
        , arrayMap(x -> x[1], categories_splitted) AS categories_main
        , arrayDistinct(arrayFlatten(categories_main)) AS categories_unique
        , title
        , price
        , related -- TODO: Investigate Clickhouse's experimental JSON type
        , nullIf(brand, '') AS brand_name
    FROM
        {{ source("raw_thirdparty_amazon", "amazon_metadata") }}
    {% if is_incremental() %}
        WHERE _meta_loaded_at > (SELECT MAX(_meta_loaded_at) FROM {{ this }})
    {% endif %}
)

, parsed AS (
    SELECT
        _meta_run_id
        , _meta_loaded_from
        , _meta_loaded_at
        , toInt32(metadata_id) AS metadata_id
        , product_id
        , sales_rank
        , image_url
        , categories_unique AS category_names
        , arrayMap(x -> {{ dbt_utils.generate_surrogate_key(['x']) }}, categories_unique) AS category_ids
        , title
        , toDecimal32OrNull(price, 2) AS price
        , related
        , brand_name
        , if(brand_name IS NULL, NULL, {{ dbt_utils.generate_surrogate_key(['brand_name']) }}) AS brand_id
    FROM raw_data
)

-- dbt_utils.deduplicate is not production ready for Clickhouse?
, deduplicated AS (
    SELECT
        *
        , ROW_NUMBER() OVER (PARTITION BY metadata_id ORDER BY _meta_loaded_at DESC) AS row_number
    FROM parsed
    QUALIFY row_number = 1
)

SELECT * EXCEPT (row_number, categories_temp, category_groups_temp) FROM deduplicated

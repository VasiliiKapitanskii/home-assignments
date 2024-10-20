{{
    config(
        engine='MergeTree',
    )
}}


WITH base AS (
    SELECT
        d.month_actual
        , d.month_name
        , c.category_name
        , AVG(f.overall_rating) AS avg_overall_rating
    FROM {{ ref('mrt_fact_product_review') }} AS f
    INNER JOIN {{ ref('mrt_dim_date') }} AS d ON f.review_date = d.date_id
    INNER JOIN {{ ref('mrt_dim_product') }} AS p ON f.product_id = p.product_id
    ARRAY JOIN p.category_ids
    INNER JOIN {{ ref('mrt_dim_category') }} AS c ON p.category_ids = c.category_id
    GROUP BY ALL
    ORDER BY d.month_actual, c.category_name
)

SELECT * EXCEPT (month_actual) FROM base

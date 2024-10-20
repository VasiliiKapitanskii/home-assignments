{{
  config(
    materialized = 'view'
    )
}} 

SELECT
    *
FROM
    {{ ref('orders') }}
WHERE order_date < {{ dbt_date.today() }}
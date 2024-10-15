SELECT 
    CAST(order_id AS INT) AS order_id,
    source,
    CAST(customer_id AS INT) AS customer_id,
    CAST(payment_id AS INT) AS payment_id,
    CAST(voucher_id AS INT) AS voucher_id,
    CAST(product_id AS INT) AS product_id,
    website,
    order_status,
    voucher_status,
    payment_status,
    TO_TIMESTAMP(order_date) AS order_date,
    TO_TIMESTAMP(payment_date) AS payment_date
FROM {{ source('raw', 'orders') }}

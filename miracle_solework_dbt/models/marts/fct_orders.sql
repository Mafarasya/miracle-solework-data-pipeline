-- Customer behavior, revenue per order, discount analysis
SELECT
    order_id,
    order_no,
    order_date,
    year,
    month,
    week,
    customer_name_clean,
    payment_method,
    shoe_brand_clean,
    clean_type,
    discount,
    discount_type,
    discount_gap,
    is_discount_missed,
    total_price,
    worker,
    order_sequence_per_customer,
    is_repeat_customer,
    expected_discount,
    discount_gap,
    is_discount_missed,
    data_source,
    is_synthetic
FROM
    {{ ref('stg_orders')}}


    SELECT *
FROM {{ ref('stg_orders') }}
WHERE discount IS NULL
   OR total_price IS NULL
   OR discount > total_price
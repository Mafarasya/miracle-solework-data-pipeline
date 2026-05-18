WITH source AS (
    SELECT * FROM {{ source('staging', 'raw_orders')}}
),

cleaned AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['order_no','order_date', 'customer_name', 'total_price', 'data_source']) }} AS order_id,
        order_no,
        order_date,
        EXTRACT(YEAR FROM order_date) AS year,
        EXTRACT(MONTH FROM order_date) AS month,
        EXTRACT(WEEK FROM order_date) AS week,
        CASE
            WHEN customer_name = 'Kost no 12'  THEN 'Pelanggan Kost 12'
            WHEN customer_name = 'Temen Putri' THEN 'Teman Putri'
            WHEN regexp_matches(customer_name, '^(Ka|Kak|Mas|Ko|Pak|Bang|Mama)\s') THEN customer_name
        ELSE customer_name
        END AS customer_name_clean,
        CASE 
            WHEN notes LIKE '%BCA%' THEN 'Transfer'
            WHEN notes LIKE '%cash' OR notes IS NULL THEN 'Cash'
            ELSE 'Unknown'
        END AS payment_method,
        CASE 
            WHEN LOWER(service_type) IN ('tas', 'karpet', 'sendal') THEN service_type
            ELSE 'sepatu'
        END AS item_type,
        CASE 
            WHEN LOWER(service_type) IN ('tas', 'karpet', 'sendal') THEN 'deep clean'
            WHEN LOWER(service_type) LIKE 'deep%' THEN 'deep clean' 
            WHEN LOWER(service_type) LIKE 'fast%' THEN 'fast clean'
            WHEN LOWER(service_type) LIKE 'reparasi%' THEN 'reparasi'
            ELSE LOWER(service_type)
        END AS clean_type,
        CASE 
            WHEN LOWER(shoe_type) IN ('on cloud', 'cloud', 'cloudtech', 'cloud tec') THEN 'On Running' 
            WHEN LOWER(shoe_type) LIKE 'nike%' THEN 'Nike'
            WHEN LOWER(shoe_type) LIKE 'adidas%' THEN 'Adidas'
            WHEN LOWER(shoe_type) IN ('nb', 'new balance') THEN 'New Balance'
            WHEN LOWER(shoe_type) LIKE 'converse%' THEN 'Converse'
            WHEN LOWER(shoe_type) IN ('gatau', '???', 'lokal') THEN 'Unknown'
            WHEN shoe_type IS NULL OR shoe_type = '' THEN 'Unknown'
            ELSE shoe_type
        END AS shoe_brand_clean,
        COALESCE(discount, 0) AS discount,
        CASE
            WHEN discount < 0 THEN 'surcharge'
            WHEN discount = 0 THEN 'none'
            WHEN discount = 5000 THEN 'special'
            WHEN discount = 8000 THEN 'promo'
            WHEN discount = 10000 THEN 'loyalty'
            WHEN discount > 10000 THEN 'manual'
            ELSE 'unknown'
        END AS discount_type,
        total_price,
        nullif(trim(notes), '') AS notes,
        worker,
        data_source,
        is_synthetic,
        CASE 
            WHEN is_synthetic = false 
                AND COALESCE(total_price, 0) = 0 THEN true
            ELSE false
        END AS is_free_service
    FROM 
        source
),

with_sequence AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_name_clean, year ORDER BY order_date
        ) AS order_sequence_per_customer
    FROM cleaned
),

with_flags AS (
    SELECT *, 
        order_sequence_per_customer > 1 AS is_repeat_customer,
        CASE
            WHEN is_free_service THEN 0
            WHEN order_sequence_per_customer % 2 = 0
                AND clean_type = 'fast clean' THEN 5000
            WHEN order_sequence_per_customer % 2 = 0
                AND clean_type = 'deep clean' THEN 10000
            ELSE 0
        END AS expected_discount
    FROM with_sequence
)

SELECT *,
    expected_discount - discount AS discount_gap,
    (expected_discount - discount) > 0 AS is_discount_missed
FROM with_flags

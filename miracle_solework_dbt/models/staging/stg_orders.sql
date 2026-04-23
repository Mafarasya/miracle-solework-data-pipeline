with source as (
    select * from {{ source('staging', 'raw_orders')}}
)

SELECT 
    order_no, -- should be updated to be surrogate_key order_id
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
        ELSE shoe_type -- keep original casing (INITCAP not available in this DuckDB version)
    END AS shoe_brand_clean,
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
    nullif(trim(notes), '') as notes,
    worker,
    data_source,
    is_synthetic
    
FROM 
    source
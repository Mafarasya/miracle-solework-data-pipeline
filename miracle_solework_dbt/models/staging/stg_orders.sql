with source as (
    select * from {{ source('staging', 'raw_orders')}}
)

select 
    order_no, -- should be updated to be surrogate_key order_id
    order_date,
    YEAR(order_date) AS year,
    MONTH(order_date) AS month,
    DAY(order_date) AS week,
    TRIM(
    regexp_replace(
      regexp_replace(customer_name, '^(?i)(ka|pak|mas|kang)\s+', ''),
      '\s*\(.*\)',
      ''
    )
  ) AS customer_name_clean,
    CASE 
        WHEN notes LIKE 'BCA' THEN 'Transfer'
        WHEN notes IS NULL THEN 'Unknown'
    END AS payment_method,
    CASE 
        WHEN LOWER(service_type) IN ("Tas", "Karpet", "Sendal") THEN service_type
        ELSE "Sepatu"
    END AS "item_type",
    CASE 
        WHEN LOWER(service_type) IN ("Tas", "Karpet", "Sendal") THEN "Deep Clean"
        WHEN LOWER(service_type) LIKE "deep%" THEN "Deep Clean" 
        WHEN LOWER(service_type) LIKE "fast%" THEN "Fast Clean"
        WHEN LOWER(service_type) LIKE "reparasi%" THEN "Reparasi"
        ELSE service_type
    END AS "clean_type",
    CASE 
        WHEN LOWER(shoe_type) IN ('on cloud', 'cloud', 'cloudtech', 'cloud tec') THEN 'On Running' 
        WHEN LOWER(shoe_type) LIKE 'nike%' THEN 'Nike'
        WHEN LOWER(shoe_type) LIKE 'adidas%' THEN 'Adidas'
        WHEN LOWER(shoe_type) IN ('nb', 'new balance') THEN 'New Balance'
        WHEN LOWER(shoe_type) LIKE 'converse%' THEN 'Converse'
        WHEN LOWER(shoe_type) IN ('gatau', '???', 'lokal') THEN 'Unknown'
        WHEN shoe_type IS NULL OR shoe_type = '' THEN 'Unknown'
        ELSE INITCAP(jenis_sepatu) -- make it capitalize as is
    END AS shoe_brand_clean
    
from 
    source
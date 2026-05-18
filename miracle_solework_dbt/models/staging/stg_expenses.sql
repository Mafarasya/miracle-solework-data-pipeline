WITH source AS (
    SELECT * FROM {{ source('staging', 'raw_expenses')}}
)

SELECT
    expense_date,
    EXTRACT(MONTH FROM expense_date) AS month,
    item_name,
    nullif(brand, '') AS brand,
    unit_price,
    quantity,
    total_price,
    (unit_price * quantity) AS calculated_total_price,
    nullif(notes, '') AS notes,
    data_source,
    (total_price != unit_price * quantity) AS is_total_price_mismatch,
    is_synthetic,
    CASE
        WHEN is_synthetic = true THEN false
        WHEN TRIM(CAST(is_date_imputed AS VARCHAR)) IN ('-', '') then false
        WHEN LOWER(TRIM(CAST(is_date_imputed AS VARCHAR))) IN ('true', '1', 'yes', 'y') THEN true
        ELSE false
    END AS is_date_imputed
FROM   
    source
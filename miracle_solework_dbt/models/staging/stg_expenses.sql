with source as (
    select * from {{ source('staging', 'raw_expenses')}}
)

SELECT
    expense_date,
    EXTRACT(MONTH FROM expense_date) AS month,
    item_name,
    nullif(brand, '') as brand,
    unit_price,
    quantity,
    total_price,
    (unit_price * quantity) AS calculated_total_price,
    nullif(notes, '') as notes,
    data_source,
    (total_price != unit_price * quantity) AS is_total_price_mismatch,
    is_synthetic,
    is_date_imputed
FROM   
    source
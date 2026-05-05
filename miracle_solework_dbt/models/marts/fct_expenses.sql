SELECT
    expense_date,
    month,
    item_name,
    brand,
    unit_price,
    quantity,
    total_price,
    calculated_total_price,
    is_total_price_mismatch,
    is_date_imputed,
    data_source,
    is_synthetic

FROM {{ ref('stg_expenses') }}
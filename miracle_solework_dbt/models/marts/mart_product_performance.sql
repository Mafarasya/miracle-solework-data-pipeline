WITH orders AS (
    SELECT *
    FROM {{ ref('stg_orders') }}
),

aggregated AS (
    SELECT
        clean_type,
        item_type,
        COUNT(*) AS total_orders,
        SUM(total_price) AS total_revenue,
        AVG(total_price) AS avg_order_value,
        SUM(discount) AS total_discount_actual,
        SUM(expected_discount) AS total_discount_expected,
        SUM(discount_gap) AS total_discount_gap,
        COUNT(CASE WHEN is_discount_missed THEN 1 END) AS total_missed_discount_count,
        COUNT(DISTINCT customer_name_clean) AS unique_customers,
        data_source,
        is_synthetic
    FROM orders
    GROUP BY clean_type,
        item_type,
        data_source,
        is_synthetic
)

SELECT *
FROM aggregated
ORDER BY total_revenue DESC, total_orders DESC
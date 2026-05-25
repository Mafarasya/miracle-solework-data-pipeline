WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

aggregated AS (
    SELECT
        customer_name_clean AS customer_name,
        count(*) AS total_orders,
        min(order_date) AS first_order_date,
        max(order_date) AS last_order_date,
        sum(total_price) AS total_revenue,
        sum(discount) AS total_discount_actual,
        sum(expected_discount) AS total_discount_expected,
        sum(discount_gap) AS total_discount_gap,
        count(CASE WHEN is_discount_missed THEN 1 END) AS total_missed_discount_count,
        data_source
    FROM orders
    GROUP BY customer_name_clean, data_source
),

with_tier AS (
    SELECT *,
        CASE
            WHEN total_orders >= 5 OR total_revenue >= 300000
                THEN 'loyal'
            ELSE 'regular'
        END AS loyalty_tier
    FROM aggregated
)

SELECT * FROM with_tier
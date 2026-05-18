WITH daily_orders AS (
    SELECT
        order_date,
        data_source,
        is_synthetic,
        SUM(total_price) AS total_revenue,
        COUNT(order_id) AS total_orders
    FROM {{ ref('fct_orders') }}
    GROUP BY
        order_date,
        data_source,
        is_synthetic
),

daily_expenses AS (
    SELECT
        expense_date,
        data_source,
        is_synthetic,
        SUM(calculated_total_price) AS total_expenses
    FROM {{ ref('fct_expenses') }}
    GROUP BY
        expense_date,
        data_source,
        is_synthetic
)

SELECT
    COALESCE(o.order_date, e.expense_date) AS date,
    COALESCE(o.data_source, e.data_source) AS data_source,
    COALESCE(o.is_synthetic, e.is_synthetic) AS is_synthetic,
    COALESCE(o.total_orders, 0) AS total_orders,
    COALESCE(o.total_revenue, 0) AS total_revenue,
    COALESCE(e.total_expenses, 0) AS total_expenses,
    COALESCE(o.total_revenue, 0) - COALESCE(e.total_expenses, 0) AS daily_profit
FROM daily_orders o
FULL OUTER JOIN daily_expenses e
    ON o.order_date = e.expense_date
   AND o.data_source = e.data_source
   AND o.is_synthetic = e.is_synthetic
ORDER BY date, data_source, is_synthetic
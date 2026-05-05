WITH daily_orders AS (
    SELECT
        order_date,
        SUM(total_price) AS total_revenue,
        COUNT(order_id) AS total_orders
    FROM {{ ref('fct_orders')}}
    GROUP BY order_date
),

daily_expenses AS (
    SELECT
        expense_date,
        SUM(calculated_total_price) as total_expenses
    FROM {{ ref('fct_expenses')}}
    GROUP BY expense_date
)

SELECT 
    COALESCE(o.order_date, e.expense_date) AS date,
    COALESCE(total_orders, 0) AS total_orders,
    COALESCE(total_revenue, 0) AS total_revenue,
    COALESCE(total_expenses, 0) AS total_expenses,
    (COALESCE(total_revenue, 0) - COALESCE(total_expenses, 0)) AS daily_profit
FROM daily_orders o
FULL OUTER JOIN daily_expenses e ON o.order_date = e.expense_date
    
-- coalesce is used to prevent nulls on either side, caused by agg SUM() function that returns null values when occured zero transaction on that day
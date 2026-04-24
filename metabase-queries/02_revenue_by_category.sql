-- ================================================
-- QUERY: Revenue by Category
-- Save as: Revenue by Category
-- Visualization: Bar Chart
-- X-axis: sub_category
-- Y-axis: total_revenue ONLY
-- ================================================
SELECT
    category,
    sub_category,
    COUNT(DISTINCT order_id)         AS total_orders,
    ROUND(SUM(sales), 2)             AS total_revenue,
    ROUND(SUM(profit), 2)            AS total_profit,
    ROUND(AVG(profit_margin_pct), 2) AS avg_margin
FROM fct_sales
GROUP BY category, sub_category
ORDER BY total_revenue DESC;

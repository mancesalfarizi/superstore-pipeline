-- ================================================
-- QUERY: Revenue by Segment
-- Save as: Revenue by Segment
-- Visualization: Bar Chart
-- X-axis: segment
-- Y-axis: total_revenue ONLY
-- ================================================
SELECT
    segment,
    COUNT(DISTINCT order_id)         AS total_orders,
    COUNT(DISTINCT customer_id)      AS total_customers,
    ROUND(SUM(sales), 2)             AS total_revenue,
    ROUND(SUM(profit), 2)            AS total_profit,
    ROUND(AVG(profit_margin_pct), 2) AS avg_margin,
    ROUND(AVG(days_to_ship), 1)      AS avg_days_to_ship
FROM fct_sales
GROUP BY segment
ORDER BY total_revenue DESC;

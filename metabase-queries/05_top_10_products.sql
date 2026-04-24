-- ================================================
-- QUERY: Top 10 Products
-- Save as: Top 10 Products
-- Visualization: Table
-- ================================================
SELECT
    p.product_name,
    f.category,
    COUNT(DISTINCT f.order_id)  AS total_orders,
    ROUND(SUM(f.sales), 2)      AS total_revenue,
    ROUND(SUM(f.profit), 2)     AS total_profit
FROM fct_sales f
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY p.product_name, f.category
ORDER BY total_revenue DESC
LIMIT 10;

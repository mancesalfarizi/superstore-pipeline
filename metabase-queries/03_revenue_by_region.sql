-- ================================================
-- QUERY: Revenue by Region
-- Save as: Revenue by Region
-- Visualization: Bar Chart
-- X-axis: region
-- Y-axis: total_revenue
-- ================================================
SELECT
    region,
    ROUND(SUM(sales), 2) AS total_revenue
FROM fct_sales
GROUP BY region
ORDER BY total_revenue DESC;

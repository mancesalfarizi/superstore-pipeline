-- ================================================
-- QUERY 1: Total Revenue
-- Save as: Total Revenue | Visualization: Number
-- ================================================
SELECT ROUND(SUM(sales), 2) AS total_revenue
FROM fct_sales;

-- ================================================
-- QUERY 2: Total Profit
-- Save as: Total Profit | Visualization: Number
-- ================================================
SELECT ROUND(SUM(profit), 2) AS total_profit
FROM fct_sales;

-- ================================================
-- QUERY 3: Total Orders
-- Save as: Total Orders | Visualization: Number
-- ================================================
SELECT COUNT(DISTINCT order_id) AS total_orders
FROM fct_sales;

-- ================================================
-- QUERY 4: Total Customers
-- Save as: Total Customers | Visualization: Number
-- ================================================
SELECT COUNT(DISTINCT customer_id) AS total_customers
FROM fct_sales;

-- ================================================
-- QUERY 5: Avg Days to Ship
-- Save as: Avg Days to Ship | Visualization: Number
-- ================================================
SELECT ROUND(AVG(days_to_ship), 1) AS avg_days_to_ship
FROM fct_sales;

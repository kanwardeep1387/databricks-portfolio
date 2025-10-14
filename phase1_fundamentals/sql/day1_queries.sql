-- Day 1 — SQL 1: Daily revenue
SELECT date, ROUND(SUM(revenue), 2) AS daily_revenue
FROM workspace.default.sample_sales
GROUP BY date
ORDER BY date;

-- Day 1 — SQL 2: Top 3 products by revenue
SELECT product, ROUND(SUM(revenue), 2) AS total_revenue
FROM workspace.default.sample_sales
GROUP BY product
ORDER BY total_revenue DESC
LIMIT 3;

-- Day 1 — SQL 3: City-level revenue
SELECT city, ROUND(SUM(revenue), 2) AS city_revenue
FROM workspace.default.sample_sales
GROUP BY city
ORDER BY city_revenue DESC;

-- Optional: Delta history peek
DESCRIBE HISTORY workspace.default.sample_sales;

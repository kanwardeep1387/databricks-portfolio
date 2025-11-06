-- Day 2(week 2) — SQL 1: Check the new column
DESCRIBE TABLE workspace.default.sample_sales;

-- Day 2 — SQL 2: 🕓 Step 2 — Time Travel
DESCRIBE HISTORY workspace.default.sample_sales;
SELECT * FROM workspace.default.sample_sales VERSION AS OF 0 LIMIT 5;
SELECT * FROM workspace.default.sample_sales TIMESTAMP AS OF "2025-10-21T10:00:00Z";

-- Day 2 — SQL 3: Step 3 — Optimize & Z-Order
OPTIMIZE workspace.default.sample_sales
ZORDER BY (date, product);

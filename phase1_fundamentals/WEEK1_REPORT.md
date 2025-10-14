# Week 1 — Day 1 Report

## Environment
- Catalog & schema: `workspace.default`
- Table: `workspace.default.sample_sales` (managed Delta via Unity Catalog)

## What I built today
- Ingested CSV from a Volume and created a managed Delta table.
- Ran three analytics queries (daily revenue, top products, city revenue).
- Viewed Delta table history.

## Results (enter brief values)
- Total rows ingested: [12]
- Top product by revenue: [Headphones]
- Highest revenue city: [Stockton]

## Notes / issues
- Avoided DBFS root. Used Unity Catalog managed table. Works fine.

## Next
- Day 2: Save a tiny SQL dashboard or screenshots, prepare for time travel + schema evolution.

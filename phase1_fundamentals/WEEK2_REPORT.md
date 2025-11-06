# Week 2 — Day 2 Report

## Objective
Practice Delta Lake advanced features: Schema Evolution, Time Travel, and Optimization.

## Steps Performed
1. Added new column `discount` using mergeSchema=true.  
2. Queried historical versions using `VERSION AS OF`.  
3. Optimized Delta table with Z-ORDER on (date, product).

## Results
- New column added successfully.  
- Table versions visible via DESCRIBE HISTORY.  
- OPTIMIZE reduced file count from __ to __.

## Learnings
- Schema evolution avoids schema mismatch errors.  
- Time travel helps data auditing and rollback.  
- Z-ORDER improves filter query performance.

## Next (Week 3)
Implement streaming ingestion and Delta Live Tables.

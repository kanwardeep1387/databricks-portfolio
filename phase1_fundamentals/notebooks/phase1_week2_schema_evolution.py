# Databricks notebook source
# MAGIC %md
# MAGIC # **Set paths and read CSV**

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

csv_path = "/Volumes/workspace/default/phase_1/sample_sales.csv" 
#delta_path = "/tmp/delta/sample_sales.delta"                # local Delta path for this lab     
#table_name = "sample_sales"                                 # will register this as a table

df = (spark.read
      .option("header", True)
      .option("inferSchema", True)
      .csv(csv_path))

display(df.limit(10))
print("Rows:", df.count())
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # Enrich & write as Delta and Register as Delta table

# COMMAND ----------

df2 = df.withColumn("loaded_at", current_timestamp())
# write to Delta (overwrite for clean reruns)
(df2.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", True)
 .save(delta_path))                     # error because writing to the public DBFS root (e.g., /tmp/delta/...) is disabled and not recommended in Databricks. You should write to a location in your workspace's cloud storage, such as an S3 bucket path

print("Delta written to:", delta_path)

# COMMAND ----------

# MAGIC %md
# MAGIC # Now write a managed table in Unity Catalog

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

table_fqn = "workspace.default.sample_sales" # <catalog>.<schema>.<table>

df2 = df.withColumn("loaded_at", current_timestamp())

# write as a managed Delta table (unity catalog)
(df2.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(table_fqn))

print("Table written:", table_fqn)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Checks

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) FROM workspace.default.sample_sales;
# MAGIC --DESCRIBE HISTORY workspace.default.sample_sales;

# COMMAND ----------

# MAGIC %md
# MAGIC # Run a few starter queries

# COMMAND ----------

# MAGIC %md
# MAGIC **Daily Revenue**

# COMMAND ----------

# MAGIC %sql
# MAGIC select date, round(sum(revenue), 2) as daily_revenue
# MAGIC from sample_sales
# MAGIC group by date
# MAGIC order by date;

# COMMAND ----------

# MAGIC %md
# MAGIC **Top 3 products by revenue**

# COMMAND ----------

# MAGIC %sql
# MAGIC select product, round(sum(revenue), 2) as total_revenue
# MAGIC from sample_sales
# MAGIC group by product
# MAGIC order by total_revenue desc
# MAGIC limit 3;

# COMMAND ----------

# MAGIC %md
# MAGIC **City-level revenue**

# COMMAND ----------

# MAGIC %sql
# MAGIC select city, round(sum(revenue), 2) as total_revenue
# MAGIC from sample_sales
# MAGIC group by city
# MAGIC order by total_revenue desc;

# COMMAND ----------

# MAGIC %md
# MAGIC **Peek at Delta History**
# MAGIC This is useful later for time travel; seeing it now helps

# COMMAND ----------

# MAGIC %sql
# MAGIC Describe history sample_sales;

# COMMAND ----------

# MAGIC %md
# MAGIC # Day 2 Objectives
# MAGIC
# MAGIC Understand schema evolution (adding new columns without breaking pipelines).
# MAGIC
# MAGIC Practice time travel – querying historical versions of your Delta table.
# MAGIC
# MAGIC Learn and apply OPTIMIZE + Z-ORDER for performance.
# MAGIC
# MAGIC Generate notebook & SQL deliverables for GitHub.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Schema Evolution (add new column)

# COMMAND ----------

# Add a new column (e.g., discount) to simulate schema evolution

from pyspark.sql.functions import rand
table_fqn = "workspace.default.sample_sales" # use same FQN as Day 1

# Read the existing Delta table
df_existing = spark.read.table(table_fqn)

# Add a new colmn 'discount'
df_new = df_existing.withColumn("discount", (rand()*0.15).cast("double"))

#write back with mergeSchema=True
(df_new.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(table_fqn))
print("Schema evolved successfully ✅")

# COMMAND ----------

# MAGIC %md
# MAGIC Check the new column:

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE workspace.default.sample_sales

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🕓 Step 2 — Time Travel
# MAGIC ## 
# MAGIC ## Each Delta write creates a version in the transaction log.

# COMMAND ----------

# MAGIC %sql
# MAGIC Describe History sample_sales;

# COMMAND ----------

# MAGIC %md
# MAGIC Then query an older version — replace 0 or 1 with your actual version:

# COMMAND ----------

# MAGIC %md
# MAGIC Version 0 of the table: without discount column, original schema

# COMMAND ----------

# MAGIC %sql
# MAGIC select* from sample_sales version as of 0 limit 5;

# COMMAND ----------

# MAGIC %md
# MAGIC Version 1 of the table: with discount column, evolved schema

# COMMAND ----------

# MAGIC %sql
# MAGIC select* from sample_sales version as of 1 limit 5;

# COMMAND ----------

# MAGIC %md
# MAGIC Query by timsestamp-based

# COMMAND ----------

# MAGIC %sql
# MAGIC select* from workspace.default.sample_sales Timestamp as of "2025-10-21T10:00:00Z";

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚡ Step 3 — Optimize & Z-Order
# MAGIC ## Compact small files and improve query performance:

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE sample_sales
# MAGIC ZORDER BY (date, product);

# COMMAND ----------

# MAGIC %md
# MAGIC 🔹 1. What is OPTIMIZE?
# MAGIC Think of it as:
# MAGIC
# MAGIC 🧩 “Repacking the small puzzle pieces into bigger, well-organized blocks.”
# MAGIC
# MAGIC The OPTIMIZE command in Delta Lake:
# MAGIC
# MAGIC Compacts many small files into a few larger Parquet files (default target ≈ 1 GB each).
# MAGIC
# MAGIC Reduces metadata overhead → faster reads.
# MAGIC
# MAGIC Improves data skipping because fewer files must be checked.
# MAGIC
# MAGIC 🔹 2. What is ZORDER BY?
# MAGIC Think of it as:
# MAGIC
# MAGIC 📚 “Re-arranging your library so related books are stored next to each other.”
# MAGIC
# MAGIC ZORDER (short for Z-ordering) is a multi-dimensional clustering technique.
# MAGIC It physically re-organizes rows within files so that data with similar values for certain columns are stored close together.

# COMMAND ----------


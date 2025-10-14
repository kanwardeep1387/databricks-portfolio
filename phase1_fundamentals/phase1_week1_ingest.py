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
# MAGIC DESCRIBE HISTORY workspace.default.sample_sales;

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


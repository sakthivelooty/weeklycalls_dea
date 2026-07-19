# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ## 
# MAGIC We have two independent data pipelines calculating summary metrics. One pipeline reads from our Oracle database, and the other reads from our Delta data lake.
# MAGIC
# MAGIC Because they run independently, their execution timestamps (run_timestamp) never match exactly, and sometimes a pipeline might run multiple times for the same data window if there was a retry.
# MAGIC
# MAGIC Additionally, the Oracle pipeline prepends the schema name (omsadm.) to the table names, while the Delta pipeline does not.
# MAGIC
# MAGIC Here is a simplified view of the data:

# COMMAND ----------

# MAGIC %md
# MAGIC ## delta_metrics

# COMMAND ----------

# MAGIC %md
# MAGIC | window_start | table_name  | row_count | run_timestamp         |
# MAGIC |--------------|-------------|-----------|-----------------------|
# MAGIC | 2026-06-01   | order_line  | 39        | 2026-07-13 20:02:12   |
# MAGIC | 2026-06-01   | order_line  | 35        | 2026-07-13 09:00:00   |

# COMMAND ----------

# MAGIC %md
# MAGIC ## oracle_metrics

# COMMAND ----------

# MAGIC %md
# MAGIC | window_start | table_name        | row_count | run_timestamp         |
# MAGIC |--------------|------------------|-----------|-----------------------|
# MAGIC | 2026-06-01   | order_line | 39        | 2026-07-13 20:01:08   |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using either SQL or PySpark, write a query to join these two tables so we can compare the row_count from both systems for each window_start and table_name combination.

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number, regexp_replace

# COMMAND ----------

delta_data = [
    ("2026-06-01", "order_line", 39, "2026-07-13 20:02:12"),
    ("2026-06-01", "order_line", 35, "2026-07-13 09:00:00"),
    ("2026-06-02", "order_line", 35, "2026-07-14 09:00:00")
]
df_delta = spark.createDataFrame(delta_data, ["window_start", "table_name", "row_count", "run_timestamp"])

# COMMAND ----------

oracle_data = [
    ("2026-06-01", "omsadm.order_line", 39, "2026-07-13 20:01:08"),
    ("2026-06-01", "omsadm.order_line", 39, "2026-07-13 22:01:08"),
    ("2026-06-01", "omsadm.order_line", 39, "2026-07-13 12:01:08"),
    ("2026-06-02", "omsadm.order_line", 39, "2026-07-14 22:01:08"),
    ("2026-06-02", "omsadm.order_line", 39, "2026-07-14 12:01:08")
]
df_oracle = spark.createDataFrame(oracle_data, ["window_start", "table_name", "row_count", "run_timestamp"])

# COMMAND ----------

window_spec = Window.partitionBy("window_start", "table_name").orderBy(col("run_timestamp").desc())

# COMMAND ----------

df_delta_latest = df_delta \
    .withColumn("rn", row_number().over(window_spec)) \
    .filter(col("rn") == 1) \
    .drop("rn")

# COMMAND ----------

df_oracle_latest = df_oracle \
    .withColumn("table_name", regexp_replace(col("table_name"), "^omsadm\\.", "")) \
    .withColumn("rn", row_number().over(window_spec)) \
    .filter(col("rn") == 1) \
    .drop("rn")

# COMMAND ----------

join_keys = ["window_start", "table_name"]

df_result = df_delta_latest.alias("d").join(
    df_oracle_latest.alias("o"),
    on=join_keys,
    how="inner"
).select(
    "window_start",
    "table_name",
    col("d.row_count").alias("delta_row_count"),
    col("o.row_count").alias("oracle_row_count"),
    col("d.run_timestamp").alias("delta_latest_run"),
    col("o.run_timestamp").alias("oracle_latest_run")
)

df_result.display()

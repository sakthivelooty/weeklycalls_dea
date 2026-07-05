# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("BroadcastAccumulatorDemo").getOrCreate()

# COMMAND ----------

# DBTITLE 1,Classic SparkContext demo

try:
    sc = spark.sparkContext
except Exception as err:
    print("This session does not allow SparkContext or RDD APIs.")
    print("Classic broadcast variables and accumulators require a non-Connect, single-user classic session.")
    print(f"Reason: {err}")


# COMMAND ----------


transactions = [
    (1, "Alice", "IN", 1200.0),
    (2, "Bob", "US", 950.0),
    (3, "Charlie", "UK", 1100.0),
    (4, "Diana", "IN", 1300.0),
    (5, "Evan", "XX", 700.0),
]

code_to_country = {
    "IN": "India",
    "US": "United States",
    "UK": "United Kingdom",
}

transactions_df = spark.createDataFrame(
    transactions,
    ["txn_id", "customer_name", "country_code", "amount"],
)

broadcast_lookup = sc.broadcast(code_to_country)
invalid_country_counter = sc.accumulator(0)


# COMMAND ----------


def lookup_country(country_code):
    country_name = broadcast_lookup.value.get(country_code)
    if country_name is None:
        invalid_country_counter.add(1)
        return "Unknown"
    return country_name

lookup_country_udf = F.udf(lookup_country, StringType())


# COMMAND ----------


result_df = (
    transactions_df
    .withColumn("country_name", lookup_country_udf(F.col("country_code")))
    .orderBy("txn_id")
)


display(result_df)


# COMMAND ----------


# Trigger execution before reading the accumulator value.
result_df.count()

print(f"2) Invalid country codes counted with accumulator: {invalid_country_counter.value}")

# Optional cleanup in long-running sessions
broadcast_lookup.unpersist()

# Databricks notebook source
# MAGIC %md
# MAGIC Broadcast Variables: Used to send a read-only variable (like a lookup table or configuration map) to each worker node once, rather than sending it with every task.

# COMMAND ----------

# MAGIC %md
# MAGIC Accumulators: Used for distributed counters or sums that can be updated in parallel across executors and read by the driver.

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

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

# COMMAND ----------

from pyspark.accumulators import AccumulatorParam

# COMMAND ----------

class StringAccumulator(AccumulatorParam):
    def zero(self, value):
        return ""

    def addInPlace(self, value1, value2):
        if not value1:
            return value2
        return value1 + ", " + value2

unknown_country_codes_acc = sc.accumulator("", StringAccumulator())

def collect_unknown_country_code(country_code):
    country_name = broadcast_lookup.value.get(country_code)
    
    if country_name is None:
        invalid_country_counter.add(1)
        unknown_country_codes_acc.add("Unknown")
        return "Unknown"
        
    else:
        unknown_country_codes_acc.add(country_name)
        return country_name


collect_unknown_udf = F.udf(collect_unknown_country_code, StringType())

dfp = transactions_df.withColumn("checked_code", collect_unknown_udf(F.col("country_code")))

print(f"Unknown country codes collected: {unknown_country_codes_acc.value}")

# COMMAND ----------

transactions_df.display()

# COMMAND ----------

dfp.count()

# COMMAND ----------

dfp.display()

# COMMAND ----------

unknown_country_codes_acc.value

# COMMAND ----------

invalid_country_counter.value

# COMMAND ----------

dfp.explain()

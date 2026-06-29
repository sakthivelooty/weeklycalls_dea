# Databricks notebook source
from pyspark.sql.functions import expr
from pyspark.sql.functions import rand

# COMMAND ----------



spark.conf.set("spark.sql.shuffle.partitions", "1")

impressions = (
  spark
    .readStream.format("rate").option("rowsPerSecond", "5").option("numPartitions", "1").load()
    .selectExpr("value AS adId", "timestamp AS impressionTime")
)



# COMMAND ----------

clicks = (
  spark
  .readStream.format("rate").option("rowsPerSecond", "5").option("numPartitions", "1").load()
  .where((rand() * 100).cast("integer") < 10)      
  .selectExpr("(value - 50) AS adId ", "timestamp AS clickTime")    
  .where("adId > 0")
)    

# COMMAND ----------

display(clicks,checkpointLocation="/Volumes/spark_kafka_projects/wikidata/wikimedia_checkpoint/weeklycalls_1")

# COMMAND ----------

display(impressions,checkpointLocation="/Volumes/spark_kafka_projects/wikidata/wikimedia_checkpoint/weeklycalls_2")

# COMMAND ----------

display(impressions.join(clicks, "adId"), checkpointLocation="/Volumes/spark_kafka_projects/wikidata/wikimedia_checkpoint/weeklycalls_2906_3")

# COMMAND ----------

# MAGIC %md
# MAGIC Inner Join with Watermarking
# MAGIC Define watermark delays on both inputs such that the engine knows how delayed the input can be.
# MAGIC  - Time range join conditions (e.g. ...JOIN ON leftTime BETWEN rightTime AND rightTime + INTERVAL 1 HOUR),
# MAGIC  - Join on event-time windows (e.g. ...JOIN ON leftTimeWindow = rightTimeWindow).
# MAGIC

# COMMAND ----------

# Define watermarks
impressionsWithWatermark = impressions \
  .selectExpr("adId AS impressionAdId", "impressionTime") \
  .withWatermark("impressionTime", "10 seconds ")

clicksWithWatermark = clicks \
  .selectExpr("adId AS clickAdId", "clickTime") \
  .withWatermark("clickTime", "20 seconds")   

# COMMAND ----------

display(
  impressionsWithWatermark.join(
    clicksWithWatermark,
    expr(""" 
      clickAdId = impressionAdId AND 
      clickTime >= impressionTime AND --Because click can only occur after impression
      clickTime <= impressionTime + interval 1 minutes  --click occur between 0-1 minutes 
      """
    )
  )
  ,
  checkpointLocation = "/Volumes/spark_kafka_projects/wikidata/wikimedia_checkpoint/weeklycalls_2906_4"
)

# COMMAND ----------

# Inner join with time range conditions
display(
  impressionsWithWatermark.join(
    clicksWithWatermark,
    expr(""" 
      clickAdId = impressionAdId AND 
      clickTime >= impressionTime AND 
      clickTime <= impressionTime + interval 1 minutes    
      """
    ),
    "leftOuter"
  ),
  checkpointLocation = "/Volumes/spark_kafka_projects/wikidata/wikimedia_checkpoint/weeklycalls_2906_5"
)

# COMMAND ----------



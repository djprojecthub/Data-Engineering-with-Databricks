# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC ### Overview
# MAGIC In this notebook, we will see how to remove duplicate records while working with structured streaming. We will apply deduplication at the silver table rather than the bronze table.
# MAGIC <br/><br/>
# MAGIC The bronze table should retain a history of the true state of our streaming source.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC ### Static read of data
# MAGIC Use spark.read() instead of spark.readStream() and check count of records.

# COMMAND ----------

(spark.read
      .table("bronze")
      .filter("topic = 'orders'")
      .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove Duplicates
# MAGIC Use dropDuplicates() to remove duplicates in static  read of data. After query executes successfully note that 20% data was duplicate.

# COMMAND ----------

from pyspark.sql import functions as F

json_schema = "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"

batch_total = (spark.read
                      .table("bronze")
                      .filter("topic = 'orders'")
                      .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
                      .select("v.*")
                      .dropDuplicates(["order_id", "order_timestamp"])
                      .count()
                )

print(batch_total)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove duplicates in streaming read
# MAGIC - In spark structured streaming, we can also use dropDuplicates() to remove duplicate data.
# MAGIC - Structured streaming can track state information for the unique keys in the data.
# MAGIC - This ensures duplicate records do not exist within or between micro batches. However, overtime this state information will scale to represent all history.
# MAGIC - We can limit the amount of state to be maintained by using **watermarking**.
# MAGIC - **Watermarking** allows only to track information for a window of time in which we expect records could be delayed.

# COMMAND ----------

# MAGIC %md
# MAGIC The below code has a watermark of 30 seconds.<br/>In this way, we make sure that there are duplicate records exists in each new microbatches to be processed.

# COMMAND ----------

deduped_df = (spark.readStream
                   .table("bronze")
                   .filter("topic = 'orders'")
                   .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
                   .select("v.*")
                   .withWatermark("order_timestamp", "30 seconds")
                   .dropDuplicates(["order_id", "order_timestamp"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### How to make sure records are not already in the target table?
# MAGIC - We learnt to remove duplicates in microbatches but what about when some records in microbatches already exists in target table?
# MAGIC - To avoid loading duplicate records in target table we can use insert-only merge.
# MAGIC - The upsert_data() function is called in each microbatch processing and the merge insert-only statement only inserts records for matching key.

# COMMAND ----------

# MAGIC %md
# MAGIC Running the below cell will register an upsert function which will be used in the stream write query to insert only new records in the silver table. 

# COMMAND ----------

def upsert_data(microBatchDF, batch):
    microBatchDF.createOrReplaceTempView("orders_microbatch")
    
    sql_query = """
      MERGE INTO orders_silver a
      USING orders_microbatch b
      ON a.order_id=b.order_id AND a.order_timestamp=b.order_timestamp
      WHEN NOT MATCHED THEN INSERT *
    """
    
    microBatchDF.sparkSession.sql(sql_query)
    #microBatchDF._jdf.sparkSession().sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the silver table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS orders_silver
# MAGIC (order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Stream
# MAGIC - **foreachBatch(upsert_data)** provides option to execute custom data writing logic on each micro batch of streaming data.

# COMMAND ----------

query = (deduped_df.writeStream
                   .foreachBatch(upsert_data)
                   .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/orders_silver")
                   .trigger(availableNow=True)
                   .start())

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Total entries check
# MAGIC Both the bronze and silver table number of entries should match.

# COMMAND ----------

streaming_total = spark.read.table("orders_silver").count()

print(f"batch total: {batch_total}")
print(f"streaming total: {streaming_total}")

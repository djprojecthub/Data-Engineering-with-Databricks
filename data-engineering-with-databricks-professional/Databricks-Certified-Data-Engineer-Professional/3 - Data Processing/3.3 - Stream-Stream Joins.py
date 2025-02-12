# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/customers_orders.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Overview
# MAGIC
# MAGIC In this notebook, we are going to see how use CDF data to propagate changes to downstream tables. We will create a `customers_orders` silver table by joining `orders` table with CDF data of `customers` table.

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC - The below query creates a `batch_upsert` function which defines a window, loads batch data into a temporary view by ranking the data using window and then runs MERGE query to update or insert changed or new records.
# MAGIC - Composite key is used in the partition and latest `_commit_timestamp` is used in each parition or group.
# MAGIC - For each batch of data the records to be insert or update is identified using `_change_type` column value.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

def batch_upsert(microBatchDF, batchId):
    window = Window.partitionBy("order_id", "customer_id").orderBy(F.col("_commit_timestamp").desc())
    
    (microBatchDF.filter(F.col("_change_type").isin(["insert", "update_postimage"]))
                 .withColumn("rank", F.rank().over(window))
                 .filter("rank = 1")
                 .drop("rank", "_change_type", "_commit_version")
                 .withColumnRenamed("_commit_timestamp", "processed_timestamp")
                 .createOrReplaceTempView("ranked_updates"))
    
    query = """
        MERGE INTO customers_orders c
        USING ranked_updates r
        ON c.order_id=r.order_id AND c.customer_id=r.customer_id
            WHEN MATCHED AND c.processed_timestamp < r.processed_timestamp
              THEN UPDATE SET *
            WHEN NOT MATCHED
              THEN INSERT *
    """
    
    microBatchDF.sparkSession.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create `customers_orders` silver table.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS customers_orders
# MAGIC (order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country STRING, row_time TIMESTAMP, processed_timestamp TIMESTAMP)

# COMMAND ----------

# MAGIC %md
# MAGIC - Streaming query to write data to `customer_orders` silver table.
# MAGIC - Reads `orders_silver` and CDF data of `customers_silver` as source into their respective dataframe.
# MAGIC - The two dataframe are then joined based on the `"customer_id"` column. This joined data is then written to `customer_orders` silver table using `foreachBatch()` operation.

# COMMAND ----------

def process_customers_orders():
    orders_df = spark.readStream.table("orders_silver")
    
    cdf_customers_df = (spark.readStream
                             .option("readChangeData", True)
                             .option("startingVersion", 2)
                             .table("customers_silver")
                       )

    query = (orders_df
                .join(cdf_customers_df, ["customer_id"], "inner")
                .writeStream
                    .foreachBatch(batch_upsert)
                    .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/customers_orders")
                    .trigger(availableNow=True)
                    .start()
            )
    
    query.awaitTermination()
    
process_customers_orders()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_orders

# COMMAND ----------

bookstore.load_new_data()
bookstore.process_bronze()
bookstore.process_orders_silver()
bookstore.process_customers_silver()

process_customers_orders()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM customers_orders

# COMMAND ----------



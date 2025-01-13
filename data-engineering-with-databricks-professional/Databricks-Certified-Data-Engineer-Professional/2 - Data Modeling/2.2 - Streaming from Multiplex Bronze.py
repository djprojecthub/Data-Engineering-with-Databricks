# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # Overview
# MAGIC In this notebook, we are going to parse raw data from single topic in our multiplex bronze table. We will create orders **silver** table.

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC Cast **key** and **value** columns to see the actual data. **Value** column has data in JSON format.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cast(key AS STRING), cast(value AS STRING)
# MAGIC FROM bronze WHERE topic='orders'
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static read
# MAGIC
# MAGIC - Reads **orders** topic data from bookstore dataset stored in bronze table. 
# MAGIC - The **from_json()** function parses the data in the given schema. 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT v.*
# MAGIC FROM (
# MAGIC   SELECT from_json(cast(value AS STRING), "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>") v
# MAGIC   FROM bronze
# MAGIC   WHERE topic = "orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming read
# MAGIC Convert the static bronze table to streaming temporary view. This allow use to write streaming queries with Spark SQL.

# COMMAND ----------

(spark.readStream
      .table("bronze")
      .createOrReplaceTempView("bronze_tmp"))

# COMMAND ----------

# MAGIC %md
# MAGIC Change the reference of above static read query to the streaming temporary view **bronze_tmp**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT v.*
# MAGIC FROM (
# MAGIC   SELECT from_json(cast(value AS STRING), "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>") v
# MAGIC   FROM bronze_tmp
# MAGIC   WHERE topic = "orders")

# COMMAND ----------

# MAGIC %md
# MAGIC Remember such always on stream prevents cluster from auto-termination.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Capture the SQL logic in a temporary view
# MAGIC We can use this temporary view to switch from SQL to Python anytime.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_silver_tmp AS
# MAGIC   SELECT v.*
# MAGIC   FROM (
# MAGIC     SELECT from_json(cast(value AS STRING), "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>") v
# MAGIC     FROM bronze_tmp
# MAGIC     WHERE topic = "orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create silver table
# MAGIC - Using streaming write function **writeStream()** to persist the result of temporary view to disk or any other cloud storage location.
# MAGIC - **trigger(availableNow=True)** specifies that all data will be processed in multiple microbatches until no more data is available and then stop the stream.

# COMMAND ----------

query = (spark.table("orders_silver_tmp")
               .writeStream
               .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/orders_silver")
               .trigger(availableNow=True)
               .table("orders_silver"))

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Complete logic using Python API

# COMMAND ----------

from pyspark.sql import functions as F

json_schema = "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"

query = (spark.readStream.table("bronze")
        .filter("topic = 'orders'")
        .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
        .select("v.*")
     .writeStream
        .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/orders_silver")
        .trigger(availableNow=True)
        .table("orders_silver"))

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM orders_silver

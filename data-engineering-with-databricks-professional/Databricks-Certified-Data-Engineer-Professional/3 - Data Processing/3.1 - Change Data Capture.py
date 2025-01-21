# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC #Overview
# MAGIC
# MAGIC ### Change Data Capture
# MAGIC CDC is process of identifying changes made to data in the source and delivering those changes to the target. Those changes could be...
# MAGIC - Inserting new records
# MAGIC - Updating existing records
# MAGIC - Deleting existing records
# MAGIC
# MAGIC Changes are logged at the source as events the contains both the data of the records and the metadata information. These metadata information records whether the specified record was inserted, updated or deleted.
# MAGIC
# MAGIC **CDC feed** = Raw data + metadata. In delta lake, you can process CDC feed using **Merge Into...** command.
# MAGIC
# MAGIC In this notebook we will create **Customer silver table**. The data in the customers topic contains complete row output from a **Change Data Capture** feed. The changes captured are either insert, update or delete.

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC - The below query unpacks **value** column of **Customers** topic and maps it to the defined schema.
# MAGIC - Load rows in dataframe **customer_df** which needs to inserted or updated.
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

schema = "customer_id STRING, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country_code STRING, row_status STRING, row_time timestamp"

customers_df = (spark.table("bronze")
                 .filter("topic = 'customers'")
                 .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
                 .select("v.*")
                 .filter(F.col("row_status").isin(["insert", "update"])))

display(customers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Notice the duplicate **customer_id** and check their **row_time** column value.

# COMMAND ----------

display(customers_df.orderBy("customer_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC - For duplicate customer_id we need to identify the most recent change which we can be done using rank function.
# MAGIC - This query only keeps the rows in **ranked_df** with latest datetime value for each customer_id because data is partitioned by customer_id and rank 1 is assigned to the row with highest row_time value.
# MAGIC - This tells the most recent operation to be applied based on the value of **row_status** column.

# COMMAND ----------

from pyspark.sql.window import Window

window = Window.partitionBy("customer_id").orderBy(F.col("row_time").desc())

ranked_df = (customers_df.withColumn("rank", F.rank().over(window))
                          .filter("rank == 1")
                          .drop("rank"))
display(ranked_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Caution**<br/>
# MAGIC This will throw an exception because non-time-based window operations are not supported on streaming DataFrames.

# COMMAND ----------

ranked_df = (spark.readStream
                   .table("bronze")
                   .filter("topic = 'customers'")
                   .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
                   .select("v.*")
                   .filter(F.col("row_status").isin(["insert", "update"]))
                   .withColumn("rank", F.rank().over(window))
                   .filter("rank == 1")
                   .drop("rank")
             )

display(ranked_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Fix**<br/>
# MAGIC To apply non-based window operation on streaming dataframe use foreachBatch logic.

# COMMAND ----------

from pyspark.sql.window import Window

def batch_upsert(microBatchDF, batchId):
    window = Window.partitionBy("customer_id").orderBy(F.col("row_time").desc())
    
    (microBatchDF.filter(F.col("row_status").isin(["insert", "update"]))
                 .withColumn("rank", F.rank().over(window))
                 .filter("rank == 1")
                 .drop("rank")
                 .createOrReplaceTempView("ranked_updates"))
    
    query = """
        MERGE INTO customers_silver c
        USING ranked_updates r
        ON c.customer_id=r.customer_id
            WHEN MATCHED AND c.row_time < r.row_time
              THEN UPDATE SET *
            WHEN NOT MATCHED
              THEN INSERT *
    """
    
    microBatchDF.sparkSession.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC Create **customers_silver** target table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS customers_silver
# MAGIC (customer_id STRING, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country STRING, row_time TIMESTAMP)

# COMMAND ----------

# MAGIC %md
# MAGIC Load country lookup data in the dataframe. We will use this to join customer information.

# COMMAND ----------

df_country_lookup = spark.read.json(f"{dataset_bookstore}/country_lookup")
display(df_country_lookup)

# COMMAND ----------

# MAGIC %md
# MAGIC #### The MAGIC!!!
# MAGIC - Performing readStream join of the customer and their country dataset. Please note that the lookup table is smaller.
# MAGIC - **Broadcast join** is an optimization technique where the smaller dataframe will be sent to all executer node in the cluster.
# MAGIC - To allow broadcast join you just need to mark which dataframe is small enough for broadcasting using the broadcast() function.
# MAGIC - This gives a hint to spark that these dataframe can fit in memory on all executors.
# MAGIC - Lastly, the foreachBatch () is executed for each batch of data.

# COMMAND ----------

query = (spark.readStream
                  .table("bronze")
                  .filter("topic = 'customers'")
                  .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
                  .select("v.*")
                  .join(F.broadcast(df_country_lookup), F.col("country_code") == F.col("code") , "inner")
               .writeStream
                  .foreachBatch(batch_upsert)
                  .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/customers_silver")
                  .trigger(availableNow=True)
                  .start()
          )

query.awaitTermination()

# COMMAND ----------

count = spark.table("customers_silver").count()
expected_count = spark.table("customers_silver").select("customer_id").distinct().count()

assert count == expected_count
print("Unit test passed.")

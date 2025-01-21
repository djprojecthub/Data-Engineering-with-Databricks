# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Overview
# MAGIC In this notebook, we will see how to enable CDF on delta lake table called `customer_silver` table and to show column highlighting table changes.
# MAGIC ### What is CDF?
# MAGIC - Delta lake Change Data Feed.
# MAGIC - Automatically generates CDC feeds about Delta Lake tables.
# MAGIC - Records row-level changes for all data written into a Delta table. [Row data + metadata (whether row was inserted, updated or deleted)]
# MAGIC - CDF is used to propagate incremental changes to downstream tables in a multi-hop architecture.
# MAGIC - CDF follows same retention policy of the table. When running VACUUM, CDF data is also deleted.
# MAGIC - **Use CDF when** table's changes include update and/or delete and small fraction of records updated in each batch.
# MAGIC - **Do not use CDF when** table's changes are append only or most records in the table updated in each batch.
# MAGIC - Small
# MAGIC - Enable CDF using command <br/>
# MAGIC > `spark.databricks.delta.properties.defaults.enableChangeDataFeed`<br/>
# MAGIC >   </t></t>OR<br/>
# MAGIC > `CREATE TABLE myTable (id INT, name STRING)
# MAGIC   TBLPROPERTIES (delta.enableChangeDataFeed = true)`

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC #### Enable CDF on existing `_customers_silver_` table

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE customers_silver 
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check `table properties` 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED customers_silver

# COMMAND ----------

# MAGIC %md
# MAGIC #### Notice that from version 2 we have CDF enabled

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY customers_silver

# COMMAND ----------

bookstore.load_new_data()

# COMMAND ----------

bookstore.process_bronze()

# COMMAND ----------

bookstore.process_orders_silver()

# COMMAND ----------

bookstore.process_customers_silver()

# COMMAND ----------

bookstore.load_new_data()
bookstore.process_bronze()
bookstore.process_orders_silver()
bookstore.process_customers_silver()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Use the `table_changes()` to show columns higlighting table changes

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM table_changes("customers_silver", 2)

# COMMAND ----------

bookstore.load_new_data()
bookstore.process_bronze()
bookstore.process_orders_silver()
bookstore.process_customers_silver()

# COMMAND ----------

bookstore.process_customers_silver()

# COMMAND ----------

# MAGIC %md
# MAGIC #### This is how we read CDF changes in PySpark.

# COMMAND ----------

cdf_df = (spark.readStream
               .format("delta")
               .option("readChangeData", True)
               .option("startingVersion", 2)
               .table("customers_silver"))

display(cdf_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Notice the `_change_data` metadata directory at customers_silver location

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/user/hive/warehouse/bookstore_eng_pro.db/customers_silver")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC #### All the CDF changes are stored in parquet format 

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/user/hive/warehouse/bookstore_eng_pro.db/customers_silver/_change_data")
display(files)

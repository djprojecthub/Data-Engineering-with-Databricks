# Databricks notebook source
# MAGIC %md
# MAGIC # Overview
# MAGIC
# MAGIC In this notebook, we will understand how partitioning is applied on bronze table in notebook `Multiplex Bronze`.

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check table location and type

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ### List out files at table location.
# MAGIC We can see the table is partitioned on topics.

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/user/hive/warehouse/bookstore_eng_pro.db/bronze")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC ### List out files at `topic=customers` partition.
# MAGIC We can see `year-month` sub-partitions.

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/user/hive/warehouse/bookstore_eng_pro.db/bronze/topic=customers")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC `year-month` partitions has the actual data and by selecting the old year-month partition you can delete the data which is not longer required.

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/user/hive/warehouse/bookstore_eng_pro.db/bronze/topic=customers/year_month=2021-12/")
display(files)

# COMMAND ----------



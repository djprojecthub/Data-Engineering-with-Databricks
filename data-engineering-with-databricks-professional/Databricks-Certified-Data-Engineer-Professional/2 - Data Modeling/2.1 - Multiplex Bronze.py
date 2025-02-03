# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/bronze.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC # Overview
# MAGIC In this notebook we will create a multiplex bronze table that stores all topics of bookstore dataset. Instead of actual kafka topic the data is being pulled from the cloud storage as per the setup of this notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## COPY DATASETS

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC ## List raw data files
# MAGIC Showing raw data at location 'kafka-raw'. Currently, we only have one raw data json file. We will use Auto loader to read the current file in this directory and detect any new file as they arrive in order to ingest them in multiplex bronze table.

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/kafka-raw")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read and display raw data
# MAGIC The raw data schema has 'value' column which contains the actual data. All other columns are like metadata for kafka write operation.

# COMMAND ----------

df_raw = spark.read.json(f"{dataset_bookstore}/kafka-raw")
display(df_raw)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Note
# MAGIC
# MAGIC The topic column shows different topics inside the bookstore dataset.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Loader in Databricks
# MAGIC
# MAGIC - Auto-loader is a mechanism in Databricks that ingests data from a data lake. The power of autoloader is that there is no need to set a trigger for ingesting new data in the data lake — it automatically pulls new files into your streaming jobs once they land in the source location.
# MAGIC - The Autoloader feature in Azure Databricks simplifies the process of loading streaming data from various sources into a Delta Lake table. It automatically detects new files in a specified directory and efficiently loads them into the table, eliminating the need for manual intervention. This enables real-time data ingestion and analysis, making it easier to build data pipelines and extract valuable insights from streaming data.
# MAGIC - It also integrates seamlessly with other services in the Azure ecosystem. You can easily ingest data from sources such as Azure Event Hubs and Azure Blob Storage, making it convenient to bring data from various sources into your Delta Lake table. 
# MAGIC - Autoloader provides options for data transformation and filtering, allowing you to preprocess your streaming data before loading it into the table. This helps streamline your data workflows and optimize data processing efficiency.
# MAGIC - It is capable of ingesting a variety of file formats, including JSON, CSV, PARQUET, AVRO, ORC, TEXT, and BINARYFILE, and can load data files from various cloud storage services such as AWS S3, Azure Data Lake Storage Gen2, Google Cloud Storage, Azure Blob Storage, ADLS Gen1, and Databricks File System.
# MAGIC - Auto Loader comes equipped with a Structured Streaming source called **cloudFiles**, which automatically processes new files as they arrive in an input directory path on the cloud file storage. This source can also process existing files in that directory. Auto Loader can support both Python and SQL in Delta Live Tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function to process raw data
# MAGIC
# MAGIC ### processBronze()
# MAGIC
# MAGIC - We start by configuring the stream to use the Autoloader by specifying the **cloudFiles** format.
# MAGIC - Then we configure the Autoloader to use JSON format and we provide schema description.
# MAGIC - Then we parse timestamp column to human readable timestamp and extract year month.
# MAGIC - **cloudFiles** format helps to detect any new file arrived in the location specified in load().- 
# MAGIC - Two new columns are added while reading and before writing the data to the table. This shows the capability to process streaming data before writing it to the table.
# MAGIC - Lastly, table is partitioned by topic and year_month
# MAGIC - Notice that we are using **mergeSchema** option to leverage the schema evolution functionality of Autoloader. This will automatically evolve the schema of the table when new fields are detected in input JSON files.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process raw data
# MAGIC
# MAGIC Let us now write a function to incrementally process raw data from source to bronze table.

# COMMAND ----------

from pyspark.sql import functions as F

def process_bronze():
  
    schema = "key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG"

    query = (spark.readStream
                        .format("cloudFiles")
                        .option("cloudFiles.format", "json")
                        .schema(schema)
                        .load(f"{dataset_bookstore}/kafka-raw")
                        .withColumn("timestamp", (F.col("timestamp")/1000).cast("timestamp"))  
                        .withColumn("year_month", F.date_format("timestamp", "yyyy-MM"))
                  .writeStream
                      .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/bronze")
                      .option("mergeSchema", True)
                      .partitionBy("topic", "year_month")
                      .trigger(availableNow=True)
                      .table("bronze"))
    
    query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run the function to process incremental batch of data
# MAGIC Since we are using **availableNow** trigger option our query executed in a batch mode. It processed all available data and then stopped on its own.

# COMMAND ----------

process_bronze()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register a dataframe using bronze table
# MAGIC
# MAGIC **spark.table("table-name")** is used to register a table as a dataframe.

# COMMAND ----------

batch_df = spark.table("bronze")
display(batch_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT(topic)
# MAGIC FROM bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ### Copy new data to source directory

# COMMAND ----------

bookstore.load_new_data()

# COMMAND ----------

process_bronze()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM bronze

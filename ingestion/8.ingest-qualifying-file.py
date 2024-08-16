# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Ingest qualifying folder

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")
v_data_source

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 1 - Read the JSON files using Spark DataFrameReader API

# COMMAND ----------

# DBTITLE 1,initial configuration variables
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# DBTITLE 1,intial common functions
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

qualifying_schema: StructType = StructType(fields = [
    StructField("qualifyId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("q1", StringType(), True),
    StructField("q2", StringType(), True),
    StructField("q3", StringType(), True),
])

# COMMAND ----------

display(dbutils.fs.ls(f"{raw_folder_path}/qualifying"))

# COMMAND ----------

from pyspark.sql import DataFrame

qualifying_df: DataFrame = (
    spark
    .read
    .format("json")
    .schema(qualifying_schema)
    .options(
        multiLine = True,
    )
    .load(f"{raw_folder_path}/qualifying/")
)

qualifying_df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 2 - Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

qualifying_final_df: DataFrame = (
    qualifying_df
    .withColumnRenamed("qualifyId", "qualify_id")
    .withColumnRenamed("raceId", "race_id")
    .withColumnRenamed("driverId", "driver_id")
    .withColumnRenamed("constructorId", "constructor_id")
    .withColumn("ingestion_date", current_timestamp())
)

qualifying_final_df.limit(5).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ##### Step 3 - Write to Processed container

# COMMAND ----------

qualifying_final_df.write.format("parquet").mode("overwrite").save(f"{processed_folder_path}/qualifying")

# COMMAND ----------

display(dbutils.fs.ls(f"{processed_folder_path}/qualifying"))

# COMMAND ----------

dbutils.notebook.exit("Success")

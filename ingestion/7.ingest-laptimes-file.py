# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Ingest lap_times folder

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")
v_data_source

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 1 - Read the CSV files using Spark DataFrameReader API

# COMMAND ----------

# DBTITLE 1,initial configuration variables
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# DBTITLE 1,intial common functions
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

lap_times_schema: StructType = StructType(fields = [
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
])

# COMMAND ----------

display(dbutils.fs.ls(f"{raw_folder_path}/lap_times"))

# COMMAND ----------

from pyspark.sql import DataFrame

lap_times_df: DataFrame = (
    spark
    .read
    .format("csv")
    .schema(lap_times_schema)
    .options(
        multiLine = False,
    )
    .load("/mnt/bossrujiformula1dl/raw/lap_times/")
)

lap_times_df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 2 - Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

lap_times_final_df: DataFrame = (
    lap_times_df
    .withColumnRenamed("raceId", "race_id")
    .withColumnRenamed("driverId", "driver_id")
    .withColumn("data_source", lit(v_data_source))
    .withColumn("ingestion_date", current_timestamp())
)

lap_times_final_df.limit(5).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ##### Step 3 - Write to Processed container

# COMMAND ----------

lap_times_final_df.write.format("parquet").mode("overwrite").save(f"{processed_folder_path}/lap_times")

# COMMAND ----------

display(dbutils.fs.ls(f"{processed_folder_path}/lap_times"))

# COMMAND ----------

dbutils.notebook.exit("Success")

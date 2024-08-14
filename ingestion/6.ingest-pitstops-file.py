# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Ingest pitstops.json file

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 1 - Read the JSON file using Spark DataFrameReader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pitstops_schema: StructType = StructType(fields = [
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("stop", StringType(), True),
    StructField("lap", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
])

# COMMAND ----------

from pyspark.sql import DataFrame

pitstops_df: DataFrame = (
    spark
    .read
    .format("json")
    .schema(pitstops_schema)
    .options(
        multiLine = True,
    )
    .load("/mnt/bossrujiformula1dl/raw/pit_stops.json")
)

pitstops_df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 2 - Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

pitstops_final_df: DataFrame = (
    pitstops_df
    .withColumnRenamed("raceId", "race_id")
    .withColumnRenamed("driverId", "driver_id")
    .withColumn("ingestion_date", current_timestamp())
)

pitstops_final_df.limit(5).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ##### Step 3 - Write to Processed container

# COMMAND ----------

pitstops_final_df.write.format("parquet").mode("overwrite").save("/mnt/bossrujiformula1dl/processed/pit_stops")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/bossrujiformula1dl/processed/pit_stops"))

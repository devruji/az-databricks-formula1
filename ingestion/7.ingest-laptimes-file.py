# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Ingest lap_times folder

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 1 - Read the CSV files using Spark DataFrameReader API

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

display(dbutils.fs.ls("/mnt/bossrujiformula1dl/raw/lap_times"))

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

from pyspark.sql.functions import current_timestamp

lap_times_final_df: DataFrame = (
    lap_times_df
    .withColumnRenamed("raceId", "race_id")
    .withColumnRenamed("driverId", "driver_id")
    .withColumn("ingestion_date", current_timestamp())
)

lap_times_final_df.limit(5).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ##### Step 3 - Write to Processed container

# COMMAND ----------

lap_times_final_df.write.format("parquet").mode("overwrite").save("/mnt/bossrujiformula1dl/processed/lap_times")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/bossrujiformula1dl/processed/lap_times"))

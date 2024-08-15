# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Ingest qualifying folder

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 1 - Read the JSON files using Spark DataFrameReader API

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

display(dbutils.fs.ls("/mnt/bossrujiformula1dl/raw/qualifying"))

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
    .load("/mnt/bossrujiformula1dl/raw/qualifying/")
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

qualifying_final_df.write.format("parquet").mode("overwrite").save("/mnt/bossrujiformula1dl/processed/qualifying")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/bossrujiformula1dl/processed/qualifying"))

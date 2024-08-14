# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Ingest results.json file

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 1 : Define the Schema and Read the JSON file using Spark DataFrameReader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, FloatType

# COMMAND ----------

results_schema: StructType = StructType(
    fields=[
        StructField("resultId", IntegerType(), False),
        StructField("raceId", IntegerType(), True),
        StructField("driverId", IntegerType(), True),
        StructField("constructorId", IntegerType(), True),
        StructField("number", IntegerType(), True),
        StructField("grid", IntegerType(), True),
        StructField("position", IntegerType(), True),
        StructField("positionText", StringType(), True),
        StructField("positionOrder", IntegerType(), True),
        StructField("points", FloatType(), True),
        StructField("laps", IntegerType(), True),
        StructField("time", StringType(), True),
        StructField("milliseconds", IntegerType(), True),
        StructField("fastestLap", IntegerType(), True),
        StructField("rank", IntegerType(), True),
        StructField("fastestLapTime", StringType(), True),
        StructField("fastestLapSpeed", FloatType(), True),
        StructField("statusId", IntegerType(), True),
    ]
)

# COMMAND ----------

from pyspark.sql import DataFrame

results_df: DataFrame = spark.read.format("JSON").schema(results_schema).load("/mnt/bossrujiformula1dl/raw/results.json")
results_df.limit(5).display()

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 2 - Rename columns and add ingestion_date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

results_renamed_df: DataFrame = (
    results_df
    .withColumnRenamed("resultId", "result_id")
    .withColumnRenamed("raceId", "race_id")
    .withColumnRenamed("driverId", "driver_id")
    .withColumnRenamed("constructorId", "constructor_id")
    .withColumnRenamed("positionText", "position_text")
    .withColumnRenamed("positionOrder", "position_order")
    .withColumnRenamed("fastestLap", "fastest_lap")
    .withColumnRenamed("fastestLapTime", "fastest_lap_time")
    .withColumnRenamed("fastestLapSpeed", "fastest_lap_Speed")
    .withColumn("ingestion_date", current_timestamp())
)

results_renamed_df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 3 - Remove the unwanted columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

results_final_df: DataFrame = results_renamed_df.drop(col("statusId"))
results_final_df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 4 - Write to Processed container

# COMMAND ----------

results_final_df.write.format("parquet").mode("overwrite").partitionBy("race_id").save("/mnt/bossrujiformula1dl/processed/results")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/bossrujiformula1dl/processed/results"))

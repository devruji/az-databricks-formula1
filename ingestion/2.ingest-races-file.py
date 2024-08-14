# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Ingest races.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step1: Read the CSV file using the spark dataframe reader

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls /mnt/bossrujiformula1dl/raw

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# COMMAND ----------

_df = spark.read.format("csv").option("header", "true").load("/mnt/bossrujiformula1dl/raw/races.csv")
_df.limit(5).display()

# COMMAND ----------

_df.printSchema()

# COMMAND ----------

races_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", StringType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True),
])

# COMMAND ----------

from pyspark.sql import DataFrame

races_df: DataFrame = (
    spark
    .read
    .options(
        header="true", 
    )
    .schema(races_schema)
    .csv("dbfs:/mnt/bossrujiformula1dl/raw/races.csv")
)

races_df.limit(5).display()

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

races_df.describe().display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step2: Select only the required columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

races_selected_df = races_df.select(
    col("raceId"),
    col("year"),
    col("round"),
    col("circuitId"),
    col("name"),
    col("date"),
    col("time")
)

races_selected_df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step3: Rename the columns as required

# COMMAND ----------

races_renamed_df: DataFrame = (
    races_selected_df
    .withColumnRenamed("raceId", "race_id")
    .withColumnRenamed("year", "race_year")
    .withColumnRenamed("circuitId", "circuit_id")
)

races_renamed_df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step4: Add race_timestamp column that transform from (date+time) as race_timestamp
# MAGIC
# MAGIC - DataFrame's API -> withColumn()
# MAGIC - SQL's function -> to_timestmp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, lit

# COMMAND ----------

_races_final_df: DataFrame = races_renamed_df.withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss"))
_races_final_df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step5: Add ingestion date to the DataFrame
# MAGIC
# MAGIC - DataFrame's API -> withColumn()
# MAGIC - Get current datetime from F.current_timestamp()

# COMMAND ----------

races_final_df = _races_final_df.withColumn("ingestion_date", current_timestamp()).drop("date", "time")

races_final_df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step5: Write data to Data Lake as Parquet format
# MAGIC
# MAGIC - [pyspark.sql.DataFrameWriter](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.html)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

races_final_df.write.mode("overwrite").parquet("/mnt/bossrujiformula1dl/processed/races")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/bossrujiformula1dl/processed/races"))

# COMMAND ----------

df = spark.read.parquet("/mnt/bossrujiformula1dl/processed/races")

df.limit(5).display()

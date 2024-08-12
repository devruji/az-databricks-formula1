# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Ingest curcuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step1: Read the CSV file using the spark dataframe reader

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls /mnt/bossrujiformula1dl/raw

# COMMAND ----------

from pyspark.sql import DataFrame

circuits_df: DataFrame = (
    spark
    .read
    .options(
        header="true", 
    )
    .csv("dbfs:/mnt/bossrujiformula1dl/raw/circuits.csv")
)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

from pyspark.sql import DataFrame

circuits_df: DataFrame = (
    spark
    .read
    .options(
        header="true", 
        inferSchema="true"
    )
    .csv("dbfs:/mnt/bossrujiformula1dl/raw/circuits.csv")
)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("alt", IntegerType(), True),
    StructField("url", StringType(), True),
])

# COMMAND ----------

from pyspark.sql import DataFrame

circuits_df: DataFrame = (
    spark
    .read
    .options(
        header="true", 
    )
    .schema(circuits_schema)
    .csv("dbfs:/mnt/bossrujiformula1dl/raw/circuits.csv")
)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

type(circuits_df)

# COMMAND ----------

circuits_df.limit(5).display()

# COMMAND ----------

circuits_df.describe().display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Select only the required columns

# COMMAND ----------

circuits_selected_df = circuits_df.select(
    "circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt"
)

# COMMAND ----------

display(circuits_selected_df.limit(5))

# COMMAND ----------

circuits_selected_df = circuits_df.select(
    circuits_df.circuitId,
    circuits_df.circuitRef,
    circuits_df.name,
    circuits_df.location,
    circuits_df.country,
    circuits_df.lat,
    circuits_df.lng,
    circuits_df.alt,
)

# COMMAND ----------

display(circuits_selected_df.limit(5))

# COMMAND ----------

circuits_selected_df = circuits_df.select(
    circuits_df["circuitId"],
    circuits_df["circuitRef"],
    circuits_df["name"],
    circuits_df["location"],
    circuits_df["country"],
    circuits_df["lat"],
    circuits_df["lng"],
    circuits_df["alt"],
)

# COMMAND ----------

display(circuits_selected_df.limit(5))

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuits_df.select(
    col("circuitId"),
    col("circuitRef"),
    col("name"),
    col("location"),
    col("country"),
    col("lat"),
    col("lng"),
    col("alt"),
)

# COMMAND ----------

display(circuits_selected_df.limit(5))

# COMMAND ----------



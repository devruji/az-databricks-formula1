# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Ingest curcuits.csv file

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")
v_data_source

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step1: Read the CSV file using the spark dataframe reader

# COMMAND ----------

# DBTITLE 1,initial configuration variables
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# DBTITLE 1,intial common functions
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls /mnt/bossrujiformula1dl/raw

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
    .csv(f"{raw_folder_path}/circuits.csv")
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
# MAGIC ### Step2: Select only the required columns

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

# MAGIC %md
# MAGIC
# MAGIC ### Step3: Rename the columns as required

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df: DataFrame = (
    circuits_selected_df
    .withColumnRenamed("circuitId", "circuit_id")
    .withColumnRenamed("circuitRef", "circuit_ref")
    .withColumnRenamed("lat", "latitude")
    .withColumnRenamed("lng", "longitude")
    .withColumnRenamed("alt", "altitude")
    .withColumn("data_source", lit(v_data_source))
)

circuits_renamed_df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step4: Add ingestion date to the DataFrame by called `add_ingestion_date()`

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

circuits_final_df = add_ingestion_date(circuits_renamed_df)

circuits_final_df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step5: Write data to Data Lake as Parquet format
# MAGIC
# MAGIC - [pyspark.sql.DataFrameWriter](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.html)

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

display(dbutils.fs.ls(f"{processed_folder_path}/circuits"))

# COMMAND ----------

df = spark.read.parquet(f"{processed_folder_path}/circuits")

df.limit(5).display()

# COMMAND ----------

dbutils.notebook.exit(value="Success")

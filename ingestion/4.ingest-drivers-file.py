# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Ingest drivers.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")
v_data_source

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 1 - Read the JSON file using the Spark DataFrameReader API

# COMMAND ----------

# DBTITLE 1,initial configuration variables
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# DBTITLE 1,intial common functions
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, DateType, StringType

# COMMAND ----------

name_schema: StructType = StructType(
    fields = [
        StructField("forename", StringType(), True),
        StructField("surname", StringType(), True)
    ]
)

drivers_schema: StructType = StructType(
    fields = [
        StructField("driverId", IntegerType(), False),
        StructField("driverRef", StringType(), True),
        StructField("number", IntegerType(), True),
        StructField("code", StringType(), True),
        StructField("name", name_schema, True),
        StructField("dob", DateType(), True),
        StructField("nationality", StringType(), True),
        StructField("url", StringType(), True),
    ]
)

# COMMAND ----------

from pyspark.sql import DataFrame

drivers_df: DataFrame = spark.read.format("json").schema(drivers_schema).load(f"{raw_folder_path}/drivers.json")
drivers_df.limit(5).display()

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 2 - Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

drivers_with_columns_df: DataFrame = (
    drivers_df
    .withColumnRenamed("driverId", "driver_id")
    .withColumnRenamed("driverRef", "driver_ref")
    .withColumn("data_source", lit(v_data_source))
    .withColumn("ingestion_date", current_timestamp())
    .withColumn("name", concat("name.forename", lit(" "), "name.surname"))
)

drivers_with_columns_df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 3 - Drop the unwanted columns

# COMMAND ----------

drivers_final_df: DataFrame = drivers_with_columns_df.drop(col("url"))
drivers_final_df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 4 - Write to processed container

# COMMAND ----------

drivers_final_df.write.format("parquet").mode("overwrite").save(f"{processed_folder_path}/drivers")

# COMMAND ----------

display(dbutils.fs.ls(f"{processed_folder_path}/drivers"))

# COMMAND ----------

dbutils.notebook.exit("Success")

# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Ingest constructors.json file

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ##### Step 1 - Reading the JSON file using Spark DataFrameReader

# COMMAND ----------

# DBTITLE 1,initial configuration variables
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# DBTITLE 1,intial common functions
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

constructors_schema: str = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC - [pyspark.sql.DataFrameReader.json](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.DataFrameReader.json.html)

# COMMAND ----------

from pyspark.sql import DataFrame

constructors_df: DataFrame = spark.read.schema(constructors_schema).format("json").load(f"{raw_folder_path}/constructors.json")
constructors_df.limit(5).display()

# COMMAND ----------

constructors_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 2 - Drop unwanted columns from the DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.drop.html

# COMMAND ----------

from pyspark.sql.functions import col

constructors_dropped_df: DataFrame = constructors_df.drop(col("url"))
constructors_dropped_df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 3 - Rename columns and add ingestion_date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructors_final_df: DataFrame = (
    constructors_dropped_df.withColumnRenamed("constructorId", "constructor_id")
    .withColumnRenamed("constructorRef", "constructor_ref")
    .withColumn("ingestion_date", current_timestamp())
)

constructors_final_df.limit(5).display()

# COMMAND ----------

# MAGIC  %md
# MAGIC
# MAGIC ##### Step 4 - Write the data

# COMMAND ----------

constructors_final_df.write.format("parquet").mode("overwrite").save(f"{processed_folder_path}/constructors")

# COMMAND ----------

display(dbutils.fs.ls(f"{processed_folder_path}/constructors"))

# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

print(raw_folder_path)
print(processed_folder_path)
print(presentation_folder_path)

# COMMAND ----------

races_df = spark.read.format("parquet").load(f"{processed_folder_path}/races")

races_df.limit(5).display()

# COMMAND ----------

races_df.filter("race_year = 2021").display()

# COMMAND ----------

races_df.where("race_year = 2021").display()

# COMMAND ----------

races_df.filter(races_df["race_year"] == 2021).display()

# COMMAND ----------

races_df.filter(races_df.race_year == 2021).display()

# COMMAND ----------

from pyspark.sql import DataFrame

races_filtered_df: DataFrame = races_df.filter((races_df.race_year == 2021) & (races_df.round < 5))
races_filtered_df.display()

# COMMAND ----------



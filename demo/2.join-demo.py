# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql import DataFrame

races_df: DataFrame = spark.read.format("parquet").load(f"{processed_folder_path}/races").withColumnRenamed("name", "races_name")
circuits_df: DataFrame = spark.read.format("parquet").load(f"{processed_folder_path}/circuits").withColumnRenamed("name", "circuits_name")

# COMMAND ----------

races_df.display()

# COMMAND ----------

circuits_df.display()

# COMMAND ----------

races_2021_df = races_df.filter(races_df.race_year == 2021)
races_2021_df.display()

# COMMAND ----------

races_circuits_df = circuits_df.join(
    other=races_df,
    on=circuits_df.circuit_id == races_df.circuit_id, 
    how="inner"
).select(
    "circuits_name",
    "location",
    "country",
    "race_id",
    "round",
    # "circuit_id",
    # "circuit_ref",
)

races_circuits_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC - Left join (LeftOuter)
# MAGIC - Right join (RightOuter)
# MAGIC - Full join (FullOuter)

# COMMAND ----------

races_df: DataFrame = spark.read.format("parquet").load(f"{processed_folder_path}/races").withColumnRenamed("name", "races_name").filter("race_year == 2021")
circuits_df: DataFrame = spark.read.format("parquet").load(f"{processed_folder_path}/circuits").withColumnRenamed("name", "circuits_name").filter("circuit_id < 70")

# COMMAND ----------

# Left

df = circuits_df.join(
    other=races_df,
    on=circuits_df.circuit_id == races_df.circuit_id, 
    how="left"
).select(
    "circuits_name",
    "location",
    "country",
    "race_id",
    "round",
    # "circuit_id",
    # "circuit_ref",
)

df.orderBy("race_id", "round").display()

# COMMAND ----------

# right

df = circuits_df.join(
    other=races_df,
    on=circuits_df.circuit_id == races_df.circuit_id, 
    how="right"
).select(
    "circuits_name",
    "location",
    "country",
    "race_id",
    "round",
    # "circuit_id",
    # "circuit_ref",
)

df.orderBy("race_id", "round").display()

# COMMAND ----------

# full

df = circuits_df.join(
    other=races_df,
    on=circuits_df.circuit_id == races_df.circuit_id, 
    how="full"
).select(
    "circuits_name",
    "location",
    "country",
    "race_id",
    "round",
    # "circuit_id",
    # "circuit_ref",
)

df.orderBy("race_id", "round").display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Semi join -> ไม่สามารถใช้ column จาก right dataframe -> return เฉพาะ data ในเจอใน left ไม่เจอใน right

# COMMAND ----------

# semi

df = circuits_df.join(
    other=races_df,
    on=circuits_df.circuit_id == races_df.circuit_id, 
    how="semi"
).select(
    "circuits_name",
    "location",
    "country",
    "race_id",
    "round"
)

df.display()

# COMMAND ----------

# semi

df = circuits_df.join(
    other=races_df,
    on=circuits_df.circuit_id == races_df.circuit_id, 
    how="semi"
).select(
    "circuits_name",
    "location",
    "country"
)

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Anti join -> return เฉพาะ data ที่ไม่เจอใน right dataframe

# COMMAND ----------

# anti

df = races_df.join(
    other=circuits_df,
    on=races_df.circuit_id == circuits_df.circuit_id, 
    how="anti"
)

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Cross join -> Catesian products

# COMMAND ----------

# anti

df = races_df.crossJoin(
    other=circuits_df
)

df.display()

# COMMAND ----------

int(df.count())

# COMMAND ----------

int(races_df.count() * circuits_df.count())

# COMMAND ----------



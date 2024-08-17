# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC ##### Relevant tables
# MAGIC
# MAGIC 1. circuits
# MAGIC 2. drivers
# MAGIC 3. constructors
# MAGIC 4. results
# MAGIC 5. races

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql import DataFrame

races_df: DataFrame = spark.read.format("parquet").load(f"{processed_folder_path}/races").withColumnRenamed("name", "race_name")
drivers_df: DataFrame = spark.read.format("parquet").load(f"{processed_folder_path}/drivers").withColumnRenamed("name", "driver_name").withColumnRenamed("number", "driver_number")
constructors_df: DataFrame = spark.read.format("parquet").load(f"{processed_folder_path}/constructors").withColumnRenamed("name", "constructor_name")
circuits_df: DataFrame = spark.read.format("parquet").load(f"{processed_folder_path}/circuits")
results_df: DataFrame = spark.read.format("parquet").load(f"{processed_folder_path}/results")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Start with results

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_date, col

# COMMAND ----------

results_df.limit(5).display()

# COMMAND ----------

results_final_df: DataFrame = (
    results_df.join(
        other=drivers_df, on=results_df.driver_id == drivers_df.driver_id, how="left"
    )
    .join(
        other=constructors_df.withColumnRenamed(
            "nationality", "constructor_nationality"
        ),
        on=results_df.constructor_id == constructors_df.constructor_id,
        how="left",
    )
    .join(other=races_df, on=results_df.race_id == races_df.race_id, how="left")
    .join(other=circuits_df, on=races_df.circuit_id == circuits_df.circuit_id)
    .withColumn("created_date", current_timestamp())
    .withColumn("race_date", to_date(col("race_timestamp"), "yyyy-MM-dd"))
    .select(
        "race_year",
        "race_name",
        "race_date",
        "location",
        "driver_name",
        "driver_number",
        "nationality",
        "constructor_name",
        "grid",
        "fastest_lap",
        "time",
        "points",
        "created_date",
    )
    .withColumnRenamed("location", "circuit_location")
    .withColumnRenamed("nationality", "driver_nationality")
    .withColumnRenamed("constructor_name", "team")
    .withColumnRenamed("time", "race_time")
)

results_final_df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Solution

# COMMAND ----------

# MAGIC %md Join races_df with circuits_df

# COMMAND ----------

races_circuits_df: DataFrame = (
    races_df.join(
        other=circuits_df,
        on=races_df.circuit_id == circuits_df.circuit_id,
        how="inner"
    )
).select(
    races_df.race_id,
    races_df.race_year,
    races_df.race_name,
    races_df.race_timestamp.alias("race_date"),
    circuits_df.location.alias("circuit_location")
)

races_circuits_df.limit(5).display()

# COMMAND ----------

# MAGIC %md Join results to all other DataFrames

# COMMAND ----------

races_results_df: DataFrame = (
    results_df.join(
        other=races_circuits_df,
        on=results_df.race_id == races_circuits_df.race_id,
        how="inner"
    ).join(
        other=drivers_df,
        on=results_df.driver_id == drivers_df.driver_id,
        how="inner"
    ).join(
        other=constructors_df,
        on=results_df.constructor_id == constructors_df.constructor_id,
        how="inner"
    )
).select(
    races_circuits_df.race_year,
    races_circuits_df.race_name,
    races_circuits_df.race_date,
    races_circuits_df.circuit_location,
    drivers_df.driver_name,
    drivers_df.driver_number,
    drivers_df.nationality.alias("driver_nationality"),
    constructors_df.constructor_name.alias("team"),
    results_df.grid,
    results_df.fastest_lap,
    results_df.time,
    results_df.points,
    current_timestamp().alias("created_date")
)

races_results_df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Compare the data from acutal in BBC [Abu Dhabi Grand Prix, Yas Marina](https://www.bbc.com/sport/formula1/2020/results)

# COMMAND ----------

from pyspark.sql.functions import desc

# COMMAND ----------

races_results_df.filter(
    condition="race_year == 2020 AND UPPER(race_name) == 'ABU DHABI GRAND PRIX'"
).orderBy(desc("points")).display()

# COMMAND ----------

races_results_df.filter(
    condition="race_year == 2020 AND UPPER(race_name) == 'ABU DHABI GRAND PRIX'"
).orderBy(races_results_df.points.desc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Write to Presentation container

# COMMAND ----------

races_results_df.write.format("parquet").save(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(dbutils.fs.ls(f"{presentation_folder_path}/race_results"))

# COMMAND ----------



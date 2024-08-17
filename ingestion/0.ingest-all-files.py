# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

v_result = dbutils.notebook.run(
    path = "1.ingest-circuits-file",
    timeout_seconds = 0,
    arguments = {
        "p_data_source": "Ergast API"
    }
)

# COMMAND ----------

v_result

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

def run_raw_notebooks(notebook_path="") -> str:
    v_result = dbutils.notebook.run(
        path = notebook_path,
        timeout_seconds = 0,
        arguments = {
            "p_data_source": "Ergast API"
        }
    )

    return v_result

# COMMAND ----------

source_name: str = "circuits"

if run_raw_notebooks(f"1.ingest-{source_name}-file").lower() == "success":
    spark.read.format("parquet").load(f"{processed_folder_path}/{source_name}").limit(5).display()

# COMMAND ----------

source_name: str = "races"

if run_raw_notebooks(f"2.ingest-{source_name}-file").lower() == "success":
    spark.read.format("parquet").load(f"{processed_folder_path}/{source_name}").limit(5).display()

# COMMAND ----------

source_name: str = "constructors"

if run_raw_notebooks(f"3.ingest-{source_name}-file").lower() == "success":
    spark.read.format("parquet").load(f"{processed_folder_path}/{source_name}").limit(5).display()

# COMMAND ----------

source_name: str = "drivers"

if run_raw_notebooks(f"4.ingest-{source_name}-file").lower() == "success":
    spark.read.format("parquet").load(f"{processed_folder_path}/{source_name}").limit(5).display()

# COMMAND ----------

source_name: str = "results"

if run_raw_notebooks(f"5.ingest-{source_name}-file").lower() == "success":
    spark.read.format("parquet").load(f"{processed_folder_path}/{source_name}/").limit(5).display()

# COMMAND ----------

source_name: str = "pitstops"

if run_raw_notebooks(f"6.ingest-{source_name}-file").lower() == "success":
    spark.read.format("parquet").load(f"{processed_folder_path}/pit_stops").limit(5).display()

# COMMAND ----------

source_name: str = "laptimes"

if run_raw_notebooks(f"7.ingest-{source_name}-file").lower() == "success":
    spark.read.format("parquet").load(f"{processed_folder_path}/lap_times").limit(5).display()

# COMMAND ----------

source_name: str = "qualifying"

if run_raw_notebooks(f"8.ingest-{source_name}-file").lower() == "success":
    spark.read.format("parquet").load(f"{processed_folder_path}/{source_name}").limit(5).display()

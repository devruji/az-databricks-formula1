# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp

def add_ingestion_date(input_df: DataFrame) -> DataFrame:
    """
    Adds a new column 'ingestion_date' to the input DataFrame with the current timestamp.

    Parameters:
    input_df (DataFrame): The input Spark DataFrame to which the 'ingestion_date' column will be added.

    Returns:
    DataFrame: A new DataFrame with an additional column 'ingestion_date' indicating the time of data ingestion.
    """
    return input_df.withColumn("ingestion_date", current_timestamp())


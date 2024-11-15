import json
from pyspark.sql import SparkSession

def load_data(config_path, spark):
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    # Reading the CSV files into Spark DataFrames using the spark session
    endorse_df = spark.read.csv(config["endorse"], header=True, inferSchema=True)
    charge_df = spark.read.csv(config["charge"], header=True, inferSchema=True)
    damages_df = spark.read.csv(config["damages"], header=True, inferSchema=True)
    primary_person_df = spark.read.csv(config["primary_person"], header=True, inferSchema=True)
    restrict_use_df = spark.read.csv(config["restrict_use"], header=True, inferSchema=True)
    units_df = spark.read.csv(config["units"], header=True, inferSchema=True)
    
    return {
        "endorse": endorse_df,
        "charge": charge_df,
        "damages": damages_df,
        "primary_person": primary_person_df,
        "restrict_use": restrict_use_df,
        "units": units_df
    }

from pyspark.sql import SparkSession
from modules.analytics_1 import run as run_analytics_1
from modules.analytics_2 import run as run_analytics_2
from modules.analytics_3 import run as run_analytics_3
from modules.analytics_4 import run as run_analytics_4
from modules.analytics_5 import run as run_analytics_5
from modules.analytics_6 import run as run_analytics_6
from modules.analytics_7 import run as run_analytics_7
from modules.analytics_8 import run as run_analytics_8
from modules.analytics_9 import run as run_analytics_9
from modules.analytics_10 import run as run_analytics_10

def load_data(spark, path):
    """
    Utility function to load CSV data into a Spark DataFrame.
    """
    return spark.read.option("header", "true").csv(path, inferSchema=True)

def get_output_path(output_config, analytics_name):
    """
    Generate the output path for the specific analytics.
    """
    return f"{output_config}/{analytics_name}"

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Crash Analytics") \
        .getOrCreate()

    # Define data paths
    primary_person_path = "data/Primary_Person_use.csv"
    units_path = "data/Units_use.csv"
    endorse_path = "data/Endorse_use.csv"
    charges_path = "data/Charges_use.csv"
    damages_path = "data/Damages_use.csv"
    restrict_use_path = "data/Restrict_use.csv"

    # Load data into DataFrames
    primary_person_df = load_data(spark, primary_person_path)
    units_df = load_data(spark, units_path)
    endorse_df = load_data(spark, endorse_path)
    charges_df = load_data(spark, charges_path)
    damages_df = load_data(spark, damages_path)
    restrict_use_df = load_data(spark, restrict_use_path)
    
    # Define output directory
    output_config = "output"

    # Run all analytics
    run_analytics_1(primary_person_df, spark, get_output_path(output_config, "analytics_1"))
    run_analytics_2(units_df, spark, get_output_path(output_config, "analytics_2"))
    run_analytics_3(primary_person_df, units_df, spark, get_output_path(output_config, "analytics_3"))
    run_analytics_4(units_df, endorse_df, spark, get_output_path(output_config, "analytics_4"))
    run_analytics_5(primary_person_df, units_df, spark, get_output_path(output_config, "analytics_5"))
    run_analytics_6(primary_person_df, units_df, spark, get_output_path(output_config, "analytics_6"))
    run_analytics_7(primary_person_df, units_df, spark, get_output_path(output_config, "analytics_7"))
    run_analytics_8(primary_person_df, units_df, spark, get_output_path(output_config, "analytics_8"))
    run_analytics_9(units_df, spark, get_output_path(output_config, "analytics_9"))
    run_analytics_10(primary_person_df, units_df, charges_df ,get_output_path(output_config, "analytics_10"))


    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()

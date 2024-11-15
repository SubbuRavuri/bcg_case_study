from pyspark.sql.functions import col

def run(units_df, spark, output_path):
    # Count how many two-wheelers are booked for crashes
    two_wheelers_booked = units_df.filter(col('VEH_BODY_STYL_ID').isin('MOTORCYCLE', 'MOTORSCOOTER', 'MOPED')) \
        .count()
    
    # Create a DataFrame for the result to save using spark.write
    result_df = spark.createDataFrame([(two_wheelers_booked,)], ["Number of Two Wheelers Booked"])
    result_df.write.mode("overwrite").option("header", "true").csv(output_path)

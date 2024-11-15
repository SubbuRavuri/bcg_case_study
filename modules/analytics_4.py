from pyspark.sql.functions import col

def run(units_df, endorse_df, spark, output_path):
    # Join units_df and endorse_df, filter conditions for hit and run with valid licenses
    valid_licenses_hit_and_run = units_df.join(endorse_df, ['CRASH_ID', 'UNIT_NBR'], 'inner') \
        .filter((col('VEH_HNR_FL') == 'Y') & (col('DRVR_LIC_ENDORS_ID').isNotNull())) \
        .count()
    
    # Create a DataFrame to store the result
    result_df = spark.createDataFrame([(valid_licenses_hit_and_run,)], ["Vehicles with Valid Licenses in Hit and Run"])
    
    # Save the result as CSV
    result_df.write.mode("overwrite").option("header", "true").csv(output_path)

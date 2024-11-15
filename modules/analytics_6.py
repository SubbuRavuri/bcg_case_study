from pyspark.sql.functions import col

def run(primary_person_df, units_df, spark, output_path):
    # Filter for injuries and death, join with units_df, group by VEH_MAKE_ID, and get top 3rd to 5th
    top_3rd_to_5th_vehicle_makes = primary_person_df.filter(col('PRSN_INJRY_SEV_ID').isin('INJURED', 'KILLED')) \
        .join(units_df, on=['CRASH_ID', 'UNIT_NBR']) \
        .groupBy('VEH_MAKE_ID') \
        .count() \
        .orderBy(col('count').desc()) \
        .limit(5).collect()[2:5]
    
    # Create a DataFrame to store the result
    result_df = spark.createDataFrame(top_3rd_to_5th_vehicle_makes, ["VEH_MAKE_ID", "Count"])
    
    # Save the result as CSV
    result_df.write.mode("overwrite").option("header", "true").csv(output_path)

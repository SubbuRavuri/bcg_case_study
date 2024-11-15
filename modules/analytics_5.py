from pyspark.sql.functions import col, count

def run(primary_person_df, units_df, spark, output_path):
    # Filter the primary_person_df for non-female involvement, join with units_df, and count accidents by state
    state_with_highest_accidents = primary_person_df.filter(col('PRSN_GNDR_ID') != 'FEMALE').select('CRASH_ID').distinct() \
        .join(units_df, on='CRASH_ID') \
        .groupBy('VEH_LIC_STATE_ID') \
        .agg(count(col('CRASH_ID')).alias('TotalCrashes')) \
        .orderBy(col('TotalCrashes').desc()) \
        .first()
    
    # Create a DataFrame to store the result
    result_df = spark.createDataFrame([(state_with_highest_accidents[0], state_with_highest_accidents[1])], 
                                      ["State", "Total Crashes"])
    
    # Save the result as CSV
    result_df.write.mode("overwrite").option("header", "true").csv(output_path)

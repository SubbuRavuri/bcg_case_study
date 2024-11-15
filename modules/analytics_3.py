from pyspark.sql.functions import col, desc

def run(primary_person_df, units_df, spark, output_path):
    # Join primary_person_df and units_df, filter conditions for driver death and airbags not deployed
    top_5_vehicle_makes = (
        primary_person_df
        .filter((col('PRSN_TYPE_ID') == 'DRIVER') & (col('PRSN_INJRY_SEV_ID') == 'KILLED') & (col('PRSN_AIRBAG_ID') == 'NOT DEPLOYED'))
        .join(units_df, on=['CRASH_ID', 'UNIT_NBR'])
        .groupBy('VEH_MAKE_ID')
        .count()
        .orderBy(desc('count'))
        .limit(5)
    )
    
    # Save the result as CSV
    top_5_vehicle_makes.write.mode("overwrite").option("header", "true").csv(output_path)

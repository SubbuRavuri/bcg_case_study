from pyspark.sql.functions import col

def run(primary_person_df, units_df, spark, output_path):
    # Filter crashes where alcohol was involved and group by driver zip code
    top_5_zip_codes_alcohol = primary_person_df.na.drop(subset=["DRVR_ZIP"]) \
        .join(units_df, ['CRASH_ID', 'UNIT_NBR']) \
        .filter(
            (col('VEH_BODY_STYL_ID').isin('PASSENGER CAR, 4-DOOR', 'SPORT UTILITY VEHICLE', 'PASSENGER CAR, 2-DOOR')) &
            (col('PRSN_ALC_RSLT_ID') == 'Positive')
        ) \
        .groupBy('DRVR_ZIP') \
        .count() \
        .orderBy(col('count').desc()) \
        .limit(5)
    
    # Save the result as CSV
    top_5_zip_codes_alcohol.write.mode("overwrite").option("header", "true").csv(output_path)

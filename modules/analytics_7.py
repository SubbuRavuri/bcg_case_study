from pyspark.sql.functions import col, count, desc, row_number
from pyspark.sql.window import Window

def run(primary_person_df, units_df, spark, output_path):
    # Join the data and group by body style and ethnicity
    top_ethnic_user_group_df = primary_person_df.join(units_df, on=['CRASH_ID', 'UNIT_NBR']) \
        .groupBy('VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID') \
        .agg(count('*').alias('count')) \
        .withColumn('rank', row_number().over(Window.partitionBy('VEH_BODY_STYL_ID').orderBy(desc('count')))) \
        .filter(col('rank') == 1) \
        .select('VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID', 'count')
    
    # Save the result as CSV
    top_ethnic_user_group_df.write.mode("overwrite").option("header", "true").csv(output_path)

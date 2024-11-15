from pyspark.sql.functions import *
from pyspark.sql import *
spark = SparkSession.builder.appName("AnalyticsProject").getOrCreate() 
def run(primary_person_df,spark, output_path):
    # Filter the crashes where males are killed, group by CRASH_ID, count, and filter where count > 2
    result = (
        primary_person_df
        .filter((col('PRSN_GNDR_ID') == 'MALE') & (col('PRSN_INJRY_SEV_ID') == 'KILLED'))
        .groupBy('CRASH_ID').count()
        .filter(col('count') > 2)
        .count()
    )
    
    # Create a DataFrame for the result to save using spark.write
    result_df = spark.createDataFrame([{"Number of Crashes": result}])
    
    # Save the result as a CSV or any other format
    result_df.write.mode("overwrite").option("header", "true").csv(output_path)

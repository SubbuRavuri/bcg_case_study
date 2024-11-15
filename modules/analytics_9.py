from pyspark.sql import functions as F

def run(units_df, spark, output_path):
    """
    Analytics 9: Count of Distinct Crash IDs where No Damaged Property was observed and 
    Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
    """
    
    # Assuming VEH_DMAG_SCL_1_ID and VEH_DMAG_SCL_2_ID are the damage severity columns.
    result = (
        units_df.filter(
            ((F.col("VEH_DMAG_SCL_1_ID") == "NO DAMAGE") & (F.col("VEH_DMAG_SCL_2_ID") == "NO DAMAGE")) |
            ((F.col("VEH_DMAG_SCL_1_ID").rlike(r"\d+") & F.col("VEH_DMAG_SCL_2_ID").rlike(r"\d+"))
             & (F.col("VEH_DMAG_SCL_1_ID").cast("int") > 4) & (F.col("VEH_DMAG_SCL_2_ID").cast("int") > 4))
        )
        .filter(F.col("FIN_RESP_TYPE_ID").contains("INSURANCE"))
        .select("CRASH_ID")
        .distinct()
        .count()
    )

    # Write the result to the output path
    result_df = spark.createDataFrame([(result,)], ["Distinct_Crash_Count"])
    result_df.write.mode('overwrite').csv(output_path, header=True)

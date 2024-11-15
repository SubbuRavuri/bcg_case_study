from pyspark.sql import functions as F

def run(primary_person_df, units_df, charges_df, output_path):
    """
    Analytics 10: Top 5 Vehicle Makes with speeding offences, licensed drivers, top vehicle colors, and top states
    """

    # Get the top 25 states with the most occurrences
    top_states = [row['DRVR_LIC_STATE_ID'] for row in primary_person_df.groupBy("DRVR_LIC_STATE_ID").count().orderBy(F.col("count").desc()).limit(25).collect()]
    
    # Get the top 10 vehicle colors
    top_colors = [row["VEH_COLOR_ID"] for row in units_df.groupBy("VEH_COLOR_ID").count().orderBy(F.col("count").desc()).limit(10).collect()]
    
    # Join the DataFrames and filter the conditions
    result = (
        charges_df
        .join(primary_person_df, on="CRASH_ID")  # Join with primary person DataFrame
        .join(units_df, on="CRASH_ID")  # Join with units DataFrame
        .filter(
            (F.col("CHARGE").like("%SPEED%")) &
            (F.col("DRVR_LIC_TYPE_ID") == "DRIVER LICENSE") &
            (F.col("VEH_COLOR_ID").isin(top_colors)) &
            (F.col("DRVR_LIC_STATE_ID").isin(top_states))
        )
        .groupBy("VEH_MAKE_ID")
        .count()
        .orderBy(F.col("count").desc())
        .limit(5)
    )
    
    # Write the result to the output path
    result.write.mode('overwrite').csv(output_path, header=True)

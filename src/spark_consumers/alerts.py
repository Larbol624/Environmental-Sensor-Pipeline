from pyspark.sql.functions import col, when,array,array_remove, size, lit,expr

def alert_check(df):
    df_alert_reason=df.withColumn(
        "alert_reasons",
        array(
            when(col("temperature").cast("double") > 26, lit("temperature too high")),
            when(col("temperature").cast("double") < 17, lit("temperature too low")),
            when(col("co2").cast("double") > 1500, lit("co2 level too high")),
            when(col("humidity").cast("double") < 40, lit("humidity too low")),
            when(col("humidity").cast("double") > 60, lit("humidity too high"))
        )
    )

    df_alert_reason = df_alert_reason.withColumn(
        "alert_reasons",
        expr("filter(alert_reasons, x -> x is not null)")
    )
    
    df_alerts=df_alert_reason.filter(size(col("alert_reasons")) > 0)

    return df_alerts
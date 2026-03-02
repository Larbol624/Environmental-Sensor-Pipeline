from pyspark.sql.window import Window
from pyspark.sql.functions import col, current_timestamp, last, lag, abs, round


def second_transform(df):

    df_deduped=df.dropDuplicates(subset=["sensor_id", "timestamp"])

    window_spec_fill=(
        Window
        .partitionBy("sensor_id")
        .orderBy("timestamp")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    
    FILL_COLUMNS=["temperature","humidity","co2"]
    for c in FILL_COLUMNS:
        df_deduped=df_deduped.withColumn(
            c,
            last(col(c),ignorenulls=True).over(window_spec_fill)
        )

    window_spec_anomaly=(
        Window
        .partitionBy("sensor_id")
        .orderBy("timestamp")
    )
    
    df_deltas = df_deduped \
        .withColumn("temp_delta", round(abs(col("temperature") - lag("temperature").over(window_spec_anomaly)),2)) \
        .withColumn("humid_delta", round(abs(col("humidity") - lag("humidity").over(window_spec_anomaly)),2)) \
        .withColumn("co2_delta", round(abs(col("co2") - lag("co2").over(window_spec_anomaly)),2))

    df_anomaly = df_deltas \
            .withColumn("temp_anomaly", col("temp_delta") > 3) \
            .withColumn("humid_anomaly", col("humid_delta") > 4) \
            .withColumn("co2_anomaly", col("co2_delta") > 100)

    df_anomaly = df_anomaly.withColumn(
        "anomaly",
        col("temp_anomaly") | col("humid_anomaly") | col("co2_anomaly")
    )

    df_final=df_anomaly.withColumn("ingest_ts",current_timestamp())

    return df_final
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_timestamp, last, lag, abs, lit, when
from pyspark.sql.types import  IntegerType, DoubleType
from pyspark.sql.window import Window
import os

os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"


spark = SparkSession.builder \
    .appName("test") \
    .master("local[*]") \
    .getOrCreate()

def clean_data(data):

    

    df=spark.createDataFrame(data)
    
    schemad_df=(
        df
        .withColumn("sensor_id",col("sensor_id").cast(IntegerType()))
        .withColumn("timestamp",to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"))
        .withColumn("temperature",col("temperature").cast(DoubleType()))
        .withColumn("humidity",col("humidity").cast(DoubleType()))
        .withColumn("co2",col("co2").cast(DoubleType()))
    )

    df_with_reason=schemad_df.withColumn(
        "dlq_reason",
        when(col("sensor_id").isNull(),"sensor_id_invalid")
        .when(col("timestamp").isNull(),"timestamp_invalid")
        .when(~col("temperature").between(-10,50),"temperature_out_of_range")
        .when(~col("humidity").between(0,100),"humidity_out_of_range")
        .when(~col("co2").between(350,5000),"co2_out_of_range")
    )

    valid_df = df_with_reason.filter(col("dlq_reason").isNull())
    dlq_df   = df_with_reason.filter(col("dlq_reason").isNotNull())
    
    df_deduped=valid_df.dropDuplicates(subset=["sensor_id", "timestamp"])

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
        .withColumn("temp_delta", abs(col("temperature") - lag("temperature").over(window_spec_anomaly))) \
        .withColumn("humid_delta", abs(col("humidity") - lag("humidity").over(window_spec_anomaly))) \
        .withColumn("co2_delta", abs(col("co2") - lag("co2").over(window_spec_anomaly)))

    df_anomaly = df_deltas \
            .withColumn("temp_anomaly", col("temp_delta") > 3) \
            .withColumn("humid_anomaly", col("humid_delta") > 4) \
            .withColumn("co2_anomaly", col("co2_delta") > 100)

    df_anomaly = df_anomaly.withColumn(
        "anomaly",
        col("temp_anomaly") | col("humid_anomaly") | col("co2_anomaly")
    )

    df_final=df_anomaly.withColumn("ingest_ts",current_timestamp())
    df_final=df_final.withColumn("source",lit("notebook"))
    df_final.show(10)
    
    return df_final, dlq_df

def test_data_minimal():
    return [{
            "sensor_id": 1,
            "timestamp": "2025-11-20T21:00:00",
            "temperature": 25.0,
            "humidity": 40.0,
            "co2": 501
            },
            {
            "sensor_id": 2,
            "timestamp": "2026-11-20T21:00:00",
            "temperature": 25.0,
            "humidity": 40.0,
            "co2": 501
            },
            {
            "sensor_id": 3,
            "timestamp": "2025-11-20T21:00:00",
            "temperature": 30.0,
            "humidity": 40.0,
            "co2": 501
            },
            {
            "sensor_id": 1,
            "timestamp": "2025-12-20T21:00:00",
            "temperature": 35.0,
            "humidity":None,
            "co2": 520
            },
            {
           "sensor_id": 2,
            "timestamp": "2025-12-20T21:00:00",
            "temperature": 18.0,
            "humidity": 20.0,
            "co2": 1900
            },
            {
           "sensor_id": 3,
            "timestamp": "2025-12-20T21:00:00",
            "temperature": 18.0,
            "humidity": 20.0,
            "co2": 1900
            },
            {
           "sensor_d": 3,
            "timestamp": "2025-11-20T21:00:00",
            "temperature": 18.0,
            "humidity": 20.0,
            "co2": 1900
            }
           ]

testdata=test_data_minimal()
clean_data(testdata)
spark.stop()
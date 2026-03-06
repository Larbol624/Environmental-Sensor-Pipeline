from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, IntegerType,DoubleType
from pyspark.sql.functions import col, when, coalesce, expr, lit, to_timestamp

def first_transform(df):

    EXPECTED_COLUMNS = ["sensor_id", "timestamp", "temperature", "humidity", "co2"]

    for column in EXPECTED_COLUMNS:
        if column not in df.columns:
            df=df.withColumn(column,lit(None))
        
    schemad_df=(
            df
            .withColumn("sensor_id", expr("try_cast(sensor_id as int)"))
            .withColumn("timestamp",
                coalesce(
                to_timestamp("timestamp"),
                to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
            ))
            .withColumn("temperature", expr("try_cast(temperature as double)"))
            .withColumn("humidity",expr("try_cast(humidity as double)"))
            .withColumn("co2",expr("try_cast(co2 as int)"))
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


    return dlq_df,valid_df
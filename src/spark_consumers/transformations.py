from pyspark.sql.functions import col, from_json, try_to_timestamp, to_json, when, coalesce, expr
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType

def clean_sensor_data(df,batch_id,output):
    
    sensor_schema = StructType([
        StructField("sensor_id", IntegerType()),
        StructField("timestamp", StringType()),
        StructField("temperature", DoubleType()),
        StructField("humidity", DoubleType()),
        StructField("co2", DoubleType())
    ])
        
    schemad_df=(
            df
            .withColumn("sensor_id",col("sensor_id").cast(IntegerType()))
            .withColumn("timestamp",
                coalesce(
                expr("try_to_timestamp(timestamp, 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS')"),
                expr("try_to_timestamp(timestamp, 'yyyy-MM-dd''T''HH:mm:ss')")
            ))
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


    

    output["valid"].append(valid_df.count())
    output["invalid"].append(dlq_df.count())


        
        
    
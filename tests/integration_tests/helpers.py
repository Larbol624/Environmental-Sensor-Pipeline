from pyspark.sql import SparkSession
import uuid
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, IntegerType,DoubleType, TimestampType

def read_writestream(spark,input_topic,output_topic,transform_func,dlq_topic=None):
    raw=(spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka_Integration:9092")
        .option("subscribe",input_topic)
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr(
    "CAST(value AS STRING) as json",
    "timestamp as kafka_timestamp"
    )
)
    schema = StructType() \
    .add("sensor_id", StringType()) \
    .add("timestamp", StringType())\
    .add("temperature", StringType())\
    .add("humidity",StringType())\
    .add("co2",StringType())

    df = (
    raw
    .withColumn("data",from_json(col("json"), schema))
    .select("data.*", "kafka_timestamp")
)
    
    if dlq_topic:
        dlq_df,cleaned_df=transform_func(df)
        dlq_df=dlq_df.selectExpr("to_json(struct(*)) AS value")

        query_dlq=(dlq_df.writeStream
            .format("kafka")
            .option("kafka.bootstrap.servers","kafka_Integration:9092")
            .option("topic",dlq_topic)
            .option("checkpointLocation", f"/tmp/checkpoints_{uuid.uuid4()}")
            .start()
    )
    else:
        cleaned_df=transform_func(df)

    cleaned_df=cleaned_df.selectExpr("to_json(struct(*)) AS value")
    
    query_clean=(cleaned_df.writeStream
            .format("kafka")
            .option("kafka.bootstrap.servers","kafka_Integration:9092")
            .option("topic",output_topic)
            .option("checkpointLocation", f"/tmp/checkpoints_{uuid.uuid4()}")
            .start()
    )

    import time
    time.sleep(5)

    query_clean.stop()
    if dlq_topic:
        query_dlq.stop()

def read_write_batch(spark,input_topic,output_topic,transform_func,dlq_topic=None):

    def proces_batch(batch_df, batch_id):

        df_transformed=transform_func(batch_df)

        df_json=df_transformed.selectExpr("to_json(struct(*)) AS value")

        df_json.write \
            .format("kafka")\
            .option("kafka.bootstrap.servers", "kafka_Integration:9092")\
            .option("topic",output_topic)\
            .save()
        


    schema=StructType() \
        .add("sensor_id",IntegerType())\
        .add("timestamp", TimestampType())\
        .add("temperature", DoubleType())\
        .add("humidity",DoubleType())\
        .add("co2",IntegerType())\
        .add("kafka_timestamp", TimestampType())

    raw=(spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka_Integration:9092")
        .option("subscribe",input_topic)
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr(
    "CAST(value AS STRING) as json",
    "timestamp as kafka_timestamp"
    )
)
    df = (
    raw
    .withColumn("data",from_json(col("json"), schema))
    .select("data.*", "kafka_timestamp")
)
    
    query=(
        df
        .writeStream
        .foreachBatch(proces_batch)
        .outputMode("append")
        .start()
    )

    

    query.awaitTermination(10)
    query.stop()
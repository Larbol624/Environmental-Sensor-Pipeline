from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType, DoubleType
from spark_consumers.transforms.second_transform import second_transform


spark = (
    SparkSession.builder \
    .appName("KafkaSparkStreaming_second_clean") \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.memory", "1g") \
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


schema = StructType() \
        .add("sensor_id", IntegerType()) \
        .add("timestamp", TimestampType())\
        .add("temperature", DoubleType())\
        .add("humidity",DoubleType())\
        .add("co2",IntegerType())


def proces_batch(batch_df,batch_id):
    
    curated_df=second_transform(batch_df)

    (
        curated_df.write
        .format("jdbc")
        .option("url", "jdbc:postgresql://postgres:5432/ESDP_db")
        .option("dbtable", "Aggregated_metrics")
        .option("user", "your_user")   ## NEED TO USE .env FILE
        .option("password", "your_password")
        .mode("append")
        .save()
    )

    json_df=curated_df.selectExpr("to_json(struct(*)) AS value")

    json_df.write \
        .format("kafka")\
        .option("kafka.bootstrap.servers", "kafka_ESDP:9092")\
        .option("topic","sensor_metrics")\
        .save()
    
raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka_ESDP:9092")
        .option("subscribe", "sensor_cleaned")
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr(
            "CAST(value AS STRING) as json"
        )
    ) 

df=(
        raw
        .withColumn("data",from_json(col("json"), schema))
        .select("data.*")
    )


query=(df.writeStream
       .foreachBatch(proces_batch)
       .option("checkpointLocation", "/tmp/metrics_checkpoint")
       .start()
       )


spark.streams.awaitAnyTermination()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType
from spark_consumers.transforms.first_transform import first_transform

spark = (
    SparkSession.builder \
    .appName("KafkaSparkStreaming_first_clean") \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.memory", "1g") \
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType() \
    .add("sensor_id", StringType()) \
    .add("timestamp", StringType())\
    .add("temperature", StringType())\
    .add("humidity",StringType())\
    .add("co2",StringType())
    

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka_ESDP:9092")
    .option("subscribe", "sensor_raw")
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

dlq_df,valid_df=first_transform(df)

valid_df=valid_df.selectExpr("to_json(struct(*)) AS value")

dlq_df=dlq_df.selectExpr("to_json(struct(*)) AS value")

query = (
    valid_df.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka_ESDP:9092")
    .option("topic", "sensor_cleaned")
    .option("checkpointLocation", "/tmp/checkpoints_valid")
    .start()
)

dlq_query= (
    dlq_df.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka_ESDP:9092")
    .option("topic", "sensor_dlq")
    .option("checkpointLocation", "/tmp/checkpoints_dlq")
    .start()
)

spark.streams.awaitAnyTermination()

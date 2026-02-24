from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, IntegerType,TimestampType, DoubleType
from alerts import alert_check

spark = (
    SparkSession.builder \
    .appName("KafkaSparkStreaming_alert") \
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
    .add("co2",IntegerType())\
    .add("kafka_timestamp",TimestampType())

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

df = (
    raw
    .withColumn("data",from_json(col("json"), schema))
    .select("data.*")
)

alert_df=alert_check(df)

alert_df=alert_df.selectExpr("to_json(struct(*)) AS value")



query = (
    alert_df.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka_ESDP:9092")
    .option("topic", "sensor_alerts")
    .option("checkpointLocation", "/tmp/checkpoints_alerts")
    .start()
)



spark.streams.awaitAnyTermination()
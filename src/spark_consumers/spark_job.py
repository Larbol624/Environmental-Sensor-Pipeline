from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, IntegerType,DoubleType
from pyspark.sql.functions import col, when, coalesce, expr, lit

spark = (
    SparkSession.builder \
    .appName("KafkaSparkStreaming") \
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





EXPECTED_COLUMNS = ["sensor_id", "timestamp", "temperature", "humidity", "co2"]

for column in EXPECTED_COLUMNS:
    if column not in df.columns:
        df=df.withColumn(column,lit(None))
    
schemad_df=(
        df
        .withColumn("sensor_id", expr("try_cast(sensor_id as int)"))
        .withColumn("timestamp",
            coalesce(
            expr("try_to_timestamp(timestamp, \"yyyy-MM-dd'T'HH:mm:ss\")"),
            expr("try_to_timestamp(timestamp, \"yyyy-MM-dd'T'HH:mm:ss.SSSSSSS\")")
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

valid_df=valid_df.selectExpr("to_json(struct(*)) AS value")

query = (
    valid_df.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka_ESDP:9092")
    .option("topic", "sensor_cleaned")
    .option("checkpointLocation", "/tmp/checkpoints")
    .start()
)

query.awaitTermination()

from pyspark.sql import SparkSession
from src.spark_consumers.first_transform import first_transform
import uuid
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, IntegerType,DoubleType

def read_writestream(spark,input_topic,output_topic,dlq_topic):
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
    dlq_df,cleaned_df=first_transform(df)

    cleaned_df=cleaned_df.selectExpr("to_json(struct(*)) AS value")
    dlq_df=dlq_df.selectExpr("to_json(struct(*)) AS value")


    query_clean=(cleaned_df.writeStream
            .format("kafka")
            .option("kafka.bootstrap.servers","kafka_Integration:9092")
            .option("topic",output_topic)
            .option("checkpointLocation", f"/tmp/checkpoints_{uuid.uuid4()}")
            .start()
    )

    query_dlq=(dlq_df.writeStream
            .format("kafka")
            .option("kafka.bootstrap.servers","kafka_Integration:9092")
            .option("topic",dlq_topic)
            .option("checkpointLocation", f"/tmp/checkpoints_{uuid.uuid4()}")
            .start()
    )

    import time
    time.sleep(5)

    query_clean.stop()
    query_dlq.stop()
    
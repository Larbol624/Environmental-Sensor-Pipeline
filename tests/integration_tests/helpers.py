from pyspark.sql import SparkSession
from first_transform import first_transform
import uuid

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
    dlq_df,cleaned_df=first_transform(raw)

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
    
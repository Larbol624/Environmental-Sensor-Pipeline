import unittest
from src.helpers.kafka import *
from src.spark_consumers.first_transform import first_transform
from src.spark_consumers.second_transform import second_transform
from src.spark_consumers.alerts import alert_check
from pyspark.sql import SparkSession
from helpers import read_writestream, read_write_batch
import time

class TestIntegration(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.producer=create_producer("kafka:9092")
        cls.delete_topics=[]
        cls.spark = SparkSession.builder \
        .master("local[2]") \
        .appName("integration-test") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .getOrCreate()

    def test_first_job(self):
        topic_raw=create_test_topic("kafka:9092")
        consumer_raw=create_consumer(topic_raw, "kafka:9092")
        self.delete_topics.append(topic_raw)

        wait_for_assignment(consumer_raw)

        self.producer.send(topic_raw,
        {
        "sensor_id": 1,
        "timestamp": '2026-01-30T21:00:00',
        "temperature": 21.0,
        "humidity": 50.0,
        "co2":500
        })
        self.producer.flush()

        response=consumer_raw.poll(500)
        msg=return_message(response)
        
        consumer_raw.close()
        


        self.assertTrue(len(msg)==1)
        self.assertEqual(msg[0]["sensor_id"],1)
        self.assertEqual(msg[0]["temperature"],21.0)
        self.assertEqual(msg[0]["timestamp"],'2026-01-30T21:00:00')
        self.assertEqual(msg[0]["humidity"],50.0)
        self.assertEqual(msg[0]["co2"],500)

        topic_raw=create_test_topic("kafka:9092")
        topic_clean=create_test_topic("kafka:9092")
        topic_dlq=create_test_topic("kafka:9092")
        self.delete_topics.append(topic_raw)
        self.delete_topics.append(topic_clean)
        self.delete_topics.append(topic_dlq)

    

        consumer_clean=create_consumer(topic_clean,bootstrap_server="kafka:9092")
        consumer_dlq=create_consumer(topic_dlq,bootstrap_server="kafka:9092")


        self.producer.send(topic_raw,
            {
            "sensor_id": 88, 
            "timestamp": '2026-03-06T18:32:17.692773', 
            "temperature": 18.58, 
            "humidity": 50.52, 
            "co2": 481
            }
        )

        self.producer.send(topic_raw,
            {
            "sensor_id": 'n',
            "timestamp": '2026-01-30T21:00:00.692773',
            "temperature": 21.0,
            "humidity": 50.0,
            "co2":500
            })
        
        self.producer.flush()

        wait_for_assignment(consumer_clean)
        wait_for_assignment(consumer_dlq)

        read_writestream(self.spark,topic_raw,topic_clean,first_transform,topic_dlq)

        response_clean=consumer_clean.poll(500)
        msg_clean=return_message(response_clean)

        response_dlq=consumer_dlq.poll(500)
        msg_dlq=return_message(response_dlq)

        self.assertTrue(len(msg_clean)==1)
        self.assertEqual(msg_clean[0]["timestamp"],"2026-03-06T18:32:17.692Z")
        self.assertTrue(msg_clean[0]["kafka_timestamp"])

        self.assertTrue(len(msg_dlq)==1)
        self.assertEqual(msg_dlq[0]["dlq_reason"],"sensor_id_invalid")

        consumer_clean.close()
        consumer_dlq.close()

    

        topic_clean=create_test_topic("kafka:9092")
        topic_alert=create_test_topic("kafka:9092")
  
        self.delete_topics.append(topic_clean)
        self.delete_topics.append(topic_alert)
        
        consumer_alert=create_consumer(topic_alert,bootstrap_server="kafka:9092")
        

        wait_for_assignment(consumer_alert)

        self.producer.send(topic_clean,{
            "sensor_id": 1,
            "timestamp": '2026-01-30T21:00:00',
            "temperature": 31.0,
            "humidity": 90.0,
            "co2":3500
            })

        self.producer.flush()

        read_writestream(self.spark,topic_clean, topic_alert,alert_check)

        response_alert=consumer_alert.poll(500)
        msg_alert=return_message(response_alert)
        consumer_alert.close()
        
        
        self.assertEqual(len(msg_alert),3)

        self.assertEqual(msg_alert[0]["alert_reason"],"temperature too high")

    

        topic_clean=create_test_topic("kafka:9092")
        topic_clean_2=create_test_topic("kafka:9092")

        self.delete_topics.append(topic_clean)
        self.delete_topics.append(topic_clean_2)

        consumer_curated=create_consumer(topic_clean_2,bootstrap_server="kafka:9092")
        wait_for_assignment(consumer_curated)

        self.producer.send(topic_clean,{
            "sensor_id": 1,
            "timestamp": '2026-01-30T21:00:00',
            "temperature": 31.0,
            "humidity": 90.0,
            "co2":3500
        })

        self.producer.send(topic_clean,{
            "sensor_id": 1,
            "timestamp": '2026-01-30T21:00:01',
            "temperature": 35.0,
            "humidity": 60.0,
            "co2":2500
        })

        read_write_batch(self.spark,topic_clean, topic_clean_2, second_transform)

        
        response_curated = None
        for _ in range(20):
            msg = consumer_curated.poll(1000)
            if msg is not None:
                response_curated = msg
                break
            time.sleep(1)

        msg_curated=return_message(response_curated)
        consumer_curated.close()

        self.assertEqual(len(msg_curated),2)
        self.assertEqual(msg_curated[1]["anomaly"],True)



    @classmethod
    def tearDown(cls):
        delete_test_topic(cls.delete_topics,bootstrap_server="kafka:9092")
        cls.producer.close()
        cls.spark.stop()
        
        

if __name__ == '__main__':
    unittest.main() 
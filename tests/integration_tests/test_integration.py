import unittest
from src.helpers.kafka import *
from pyspark.sql import SparkSession
from helpers import read_writestream

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
            "sensor_id": 1,
            "timestamp": '2026-01-30T21:00:00',
            "temperature": 21.0,
            "humidity": 50.0,
            "co2":500
            }
        )

        self.producer.send(topic_raw,
            {
            "sensor_id": 'n',
            "timestamp": '2026-01-30T21:00:00',
            "temperature": 21.0,
            "humidity": 50.0,
            "co2":500
            })
        
        self.producer.flush()

        wait_for_assignment(consumer_clean)
        wait_for_assignment(consumer_dlq)

        read_writestream(self.spark,topic_raw,topic_clean,topic_dlq)

        response_clean=consumer_clean.poll(500)
        msg_clean=return_message(response_clean)

        response_dlq=consumer_dlq.poll(500)
        msg_dlq=return_message(response_dlq)

        self.assertTrue(len(msg_clean)==1)
        self.assertEqual(msg_clean[0]["timestamp"],'2026-01-30T21:00:00.000Z')
        self.assertTrue(msg_clean[0]["kafka_timestamp"])

        self.assertTrue(len(msg_dlq)==1)
        self.assertEqual(msg_dlq[0]["dlq_reason"],"sensor_id_invalid")

        consumer_clean.close()
        consumer_dlq.close()


    @classmethod
    def tearDown(cls):
        delete_test_topic(cls.delete_topics,bootstrap_server="kafka:9092")
        cls.producer.close()
        cls.spark.stop()
        
        

if __name__ == '__main__':
    unittest.main() 
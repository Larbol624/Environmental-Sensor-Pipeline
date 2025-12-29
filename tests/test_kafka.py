import unittest
import json
import time
from unittest.mock import MagicMock
from src.Generating_Data.generate_data import test_stream
from kafka import KafkaConsumer
from src.helpers.kafka import *



class TestKafka(unittest.TestCase):
    def test_kafka_end_to_end(self):

        producer=create_producer()
        consumer = KafkaConsumer(
        "sensor_raw",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=5000
    )
        
        producer.send("sensor_raw",{"test":10})
        producer.flush()

        msg_received = False
        timeout = 10 
        start = time.time()

        while time.time() - start < timeout:
            records = consumer.poll(timeout_ms=500)  
            for tp, msgs in records.items():
                for msg in msgs:
                    if msg.value == {"test": 10}:
                        msg_received = True
                        break
                if msg_received:
                    break
            if msg_received:
                break
            time.sleep(0.2) 
        assert msg_received, "Kafka message is nooit ontvangen!"

        self.assertTrue(msg_received)
        return
    
    def test_producer(self):
        consumer=KafkaConsumer(
            "sensor_raw",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            consumer_timeout_ms=5000
        )
        producer=create_producer()
        test_stream(producer)
        result=consumer.poll(500)
        self.assertIsNotNone(result)
        

    
if __name__ == '__main__':
    unittest.main() 
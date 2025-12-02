import unittest
import json
from unittest.mock import MagicMock
from src.Generating_Data.generate_data import test_stream
from kafka import KafkaConsumer
from src.helpers.kafka import *
from src.spark_consumers.Clean_data import *


class TestKafka(unittest.TestCase):
    def test_kafka_end_to_end(self):
        ## Create a producer and consumer
        producer=create_producer()
        consumer = KafkaConsumer(
        "test_topic",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
        
        ## Send a test message
        producer.send("test_topic",{"test":10})
        producer.flush()

        ## check if the message is recieved
        msg=next(consumer)
        self.assertEqual(msg.value,{"test":10})
        return
    
    def test_sensor_data_cleaning(self):
        raw={
            "sensor_id"  : "10",
            "Timestamp"  : "2025-11-20T21:00:00",
            "Temperature" : "20",
            "Humidity"   : "40",
            "Co2"        : "500"
        }

        cleaned=clean_sensor_data(raw)
        self.assertEqual(cleaned["sensor_id"], "10")
        self.assertEqual(cleaned["Temperature"], "20")
        self.assertEqual(cleaned["Humidity"], "40")
        self.assertEqual(cleaned["Timestamp"], "2025-11-20T21:00:00")
        self.assertEqual(cleaned["Co2"], "500")
        

        msg = MagicMock()
        msg.value = raw

        mock_producer = MagicMock()
        output_topic = "sensor_cleaned"

        cleaned= process_raw_data(msg,mock_producer,output_topic)

        self.assertEqual(cleaned["sensor_id"], "10")
        self.assertEqual(cleaned["Temperature"], "20")
        self.assertEqual(cleaned["Humidity"], "40")
        self.assertEqual(cleaned["Timestamp"], "2025-11-20T21:00:00")
        self.assertEqual(cleaned["Co2"], "500")

        mock_producer.send.assert_called_once_with(
            output_topic,
            cleaned
        )
        
        return
    
if __name__ == '__main__':
    unittest.main() 
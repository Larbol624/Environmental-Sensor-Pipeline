import unittest
from kafka import KafkaConsumer,KafkaProducer
import json

class TestSpark(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.producer=KafkaProducer(
            bootstrap_servers="127.0.0.1:29092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        cls.consumer_cleaned=KafkaConsumer(
            "sensor_cleaned",
            bootstrap_servers="127.0.0.1:29092",
            auto_offset_reset="latest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            consumer_timeout_ms=5000
        )

    def test_spark_up(self):

        test_data={"sensor_id": 1,"timestamp": "2025-11-20T21:00:00","temperature": 25.0,"humidity": 40.0,"co2": 501}

        self.producer.send("sensor_raw",test_data)
        self.producer.flush()

        response=self.consumer_cleaned.poll(5000)
        if response:
            tp=next(iter(response))
            msg=response[tp][0]

        self.assertEqual(msg.value,{'sensor_id': 1,'timestamp': '2025-11-20T21:00:00.000Z','temperature': 25.0,'humidity': 40.0,'co2': 501})

    @classmethod
    def tearDownClass(cls):
        cls.producer.close()
        cls.consumer_cleaned.close()


if __name__ == '__main__':
    unittest.main() 
            
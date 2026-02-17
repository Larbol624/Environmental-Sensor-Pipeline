import unittest
from src.helpers.kafka import create_producer, create_consumer, return_message, wait_for_assignment

class TestIntegration(unittest.TestCase):

    ## MAYBE MAKE TEST_TOPICS IF THIS DOESN'T WORK
    @classmethod
    def setUpClass(cls):
        cls.producer=create_producer()
        cls.consumer_cleaned=create_consumer("sensor_cleaned")
        cls.consumer_raw=create_consumer("sensor_raw")
 
        
    def test_first_job(self):

        wait_for_assignment(self.consumer_raw)
        wait_for_assignment(self.consumer_cleaned)

        self.producer.send("sensor_raw",
        {
        "sensor_id": 1,
        "timestamp": '2026-01-30T21:00:00',
        "temperature": 21.0,
        "humidity": 50.0,
        "co2":500
        })

        self.producer.flush()

        result_raw= self.consumer_raw.poll(timeout_ms=500)
        response=return_message(result_raw)
        result_cleaned= self.consumer_cleaned.poll(timeout_ms=500)


        self.assertIsNotNone(result_raw)
        self.assertIsNotNone(result_cleaned)

        

        self.assertEqual(response,
        [{
        "sensor_id": 1,
        "timestamp": '2026-01-30T21:00:00',
        "temperature": 21.0,
        "humidity": 50.0,
        "co2":500
        }])


    @classmethod
    def tearDown(cls):
        cls.producer.close()
        cls.consumer_cleaned.close()
        cls.consumer_raw.close()


if __name__ == '__main__':
    unittest.main() 
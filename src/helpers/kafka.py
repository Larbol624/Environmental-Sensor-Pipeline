from kafka import KafkaProducer
import json

def create_producer():
    return KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

def send_sensor_data(topic,data,producer=None):
    if producer is None:
        producer=create_producer()
    return producer.send(topic,data)
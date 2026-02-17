from kafka import KafkaProducer, KafkaConsumer
import json
import uuid
import time

def create_producer():
    return KafkaProducer(
        bootstrap_servers="127.0.0.1:29092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

def send_sensor_data(topic,data,producer=None):
    if producer is None:
        producer=create_producer()
    return producer.send(topic,data)


def create_consumer(topic):
    return KafkaConsumer(
        topic,
        group_id = f"test-{uuid.uuid4()}",
        auto_offset_reset="earliest",
        enable_auto_commit = False,
        bootstrap_servers="127.0.0.1:29092",
        value_deserializer=lambda v:json.loads(v.decode("utf-8"))
    )

def wait_for_assignment(consumer, timeout=5):
        start = time.time()
        while not consumer.assignment():
            consumer.poll(100)
            if time.time() - start > timeout:
                raise TimeoutError("Partition assignment failed")
            


def return_message(result):
    response=[]
    for tp,msgs in result.items():
        for msg in msgs:
            if msg.value: 
                response.append(msg.value)
    return response
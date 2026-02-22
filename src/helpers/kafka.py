from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
import json
import uuid
import time

def create_producer(bootstrap_server="127.0.0.1:29092"):
    return KafkaProducer(
        bootstrap_servers=bootstrap_server,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

def send_sensor_data(topic,data,producer=None):
    if producer is None:
        producer=create_producer()
    return producer.send(topic,data)


def create_consumer(topic,bootstrap_server="127.0.0.1:29092"):
    return KafkaConsumer(
        topic,
        group_id = f"test-{uuid.uuid4()}",
        auto_offset_reset="earliest",
        enable_auto_commit = False,
        bootstrap_servers=bootstrap_server,
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


def create_test_topic(bootstrap_server="127.0.0.1:29092"):
    admin=KafkaAdminClient(bootstrap_servers=bootstrap_server)
    topic_name=f"test_topic_{uuid.uuid4().hex[:6]}"
    topic=NewTopic(name=topic_name,num_partitions=1,replication_factor=1)
    admin.create_topics([topic])
    admin.close()
    return topic_name


def delete_test_topic(topics,bootstrap_server="127.0.0.1:29092"):
    admin=KafkaAdminClient(bootstrap_servers=bootstrap_server)
    admin.delete_topics(topics)
    admin.close()
    

from kafka import KafkaConsumer


def test_consume():
    consumer = KafkaConsumer(
        "test_topic",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest"
    )

    for msg in consumer:
        print(msg.value.decode('utf-8'))
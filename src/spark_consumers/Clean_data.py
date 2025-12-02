from kafka import KafkaConsumer,KafkaProducer

RAW_TOPIC="sensor_raw"
CLEAN_TOPIC="sensor_cleaned"
BOOTSTRAP="localhost:9092"


def clean_sensor_data(data):

    return data

def process_raw_data(data, producer, output_topic):
    cleaned=clean_sensor_data(data.value)
    producer.send(output_topic, cleaned)
    return cleaned

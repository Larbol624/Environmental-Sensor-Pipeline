import random
import time
import json
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers="localhost:9092")


NUM_SENSORS = 100

# Startwaarden: elke sensor krijgt een basiswaarde
sensor_state = {
    sensor_id: {
        "temperature": random.uniform(18.0, 23.0),
        "humidity": random.uniform(40.0, 60.0),
        "co2": random.uniform(400, 600)
    }
    for sensor_id in range(1, NUM_SENSORS + 1)
}

def update_value(value, min_val, max_val, max_change):
    """
    value: huidige waarde
    min_val, max_val: totaal bereik
    max_change: maximale verandering per seconde
    """
    change = random.uniform(-max_change, max_change)
    new_value = value + change
    return max(min(new_value, max_val), min_val)

def generate_sensor_reading(sensor_id):
    state = sensor_state[sensor_id]

    # Realistische, kleine veranderingen per seconde
    state["temperature"] = update_value(state["temperature"], 15, 30, 0.3)
    state["humidity"] = update_value(state["humidity"], 30, 90, 1.0)
    state["co2"] = update_value(state["co2"], 350, 1500, 10)

    return {
        "sensor_id": sensor_id,
        "timestamp": datetime.now().isoformat(),
        "temperature": round(state["temperature"], 2),
        "humidity": round(state["humidity"], 2),
        "co2": int(state["co2"])
    }

def live_stream():
    print("Starting realistic live sensor stream...\n")
    for i in range(10):
        for sensor_id in range(1, NUM_SENSORS + 1):
            reading = generate_sensor_reading(sensor_id)
            producer.send("sensor_raw", json.dumps(reading).encode('utf-8'))
            producer.flush()
        time.sleep(1)

def test_stream(test_producer):
    for sensor_id in range(1, NUM_SENSORS + 1):
            reading = generate_sensor_reading(sensor_id)
            test_producer.send("sensor_raw",reading)
            test_producer.flush()

if __name__ == "__main__":
    live_stream()

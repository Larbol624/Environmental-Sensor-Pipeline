from kafka import KafkaProducer
import random
import time
import json
from datetime import datetime, timedelta


producer =KafkaProducer(bootstrap_servers="kafka:9092")

def update_state(state):
    state["temperature"]+=random.uniform(-0.5,0.5)
    state["humidity"]+=random.uniform(-1,1)
    state["co2"]+=random.uniform(-50,50)
    state["timestamp"]+=timedelta(seconds=1)
    return state


sensor_state=[]
for i in range(1,11):
    sensor_state.append({
                         "sensor_id":i,
                         "timestamp":datetime(2026,8,3,21,0,0,0),
                         "temperature":random.uniform(18.0,25.0),
                         "humidity":random.uniform(45.0,55.0),
                         "co2":random.uniform(600,2000)
                        }
                       )

for _ in range(1000):
    for i, state in enumerate(sensor_state):
        sensor_state[i]=update_state(state)
    
        kafka_json=json.dumps({
            "sensor_id"     :   state["sensor_id"],
            "timestamp"     :   state["timestamp"].isoformat(),
            "temperature"   :   round(state["temperature"],2),
            "humidity"       :   round(state["humidity"],2),
            "co2"           :   round(state["co2"])
        }).encode('utf-8')

        producer.send("sensor_raw",kafka_json)
        producer.flush()
        
    time.sleep(1)

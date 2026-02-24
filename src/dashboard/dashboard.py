import streamlit as st
from kafka import KafkaConsumer
import json
import pandas as pd

st.title("Live Alerts Dashboard")

consumer = KafkaConsumer(
    "sensor_alerts",
    bootstrap_servers="kafka:9092",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

alerts = []

placeholder = st.empty()

for message in consumer:
    alerts.append(message.value)
    df = pd.DataFrame(alerts)
    placeholder.dataframe(df)
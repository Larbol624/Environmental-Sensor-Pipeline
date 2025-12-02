kafka-topics.sh --create \
  --topic sensor_raw \
  --bootstrap-server localhost:9092 \
  --partitions 6 \
  --replication-factor 1

kafka-topics.sh --create \
  --topic sensor_cleaned \
  --bootstrap-server localhost:9092 \
  --partitions 6 \
  --replication-factor 1

kafka-topics.sh --create \
  --topic sensor_metrics \
  --bootstrap-server localhost:9092 \
  --partitions 6 \
  --replication-factor 1

kafka-topics.sh --create \
  --topic sensor_alerts \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
  
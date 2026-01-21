echo"[INFO] CREATE_TOPICS is doing something"

/usr/bin/kafka-topics --create \
  --topic sensor_raw \
  --bootstrap-server localhost:9092 \
  --partitions 6 \
  --replication-factor 1 \
  --if-not-exists

/usr/bin/kafka-topics --create \
  --topic sensor_cleaned \
  --bootstrap-server localhost:9092 \
  --partitions 6 \
  --replication-factor 1 \
  --if-not-exists

/usr/bin/kafka-topics --create \
  --topic sensor_metrics \
  --bootstrap-server localhost:9092 \
  --partitions 6 \
  --replication-factor 1 \
  --if-not-exists

/usr/bin/kafka-topics --create \
  --topic sensor_alerts \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

/usr/bin/kafka-topics --create \
  --topic sensor_dlq \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

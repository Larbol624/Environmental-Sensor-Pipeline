Sensor Producer ----> Kafka (sensor_raw) ----> spark_consumer  ----> Kafka (sensor_cleaned)


First a producer container produces 10 readings from a sensor individually to a topic called sensor_raw in Kafka.
Then a spark consumers reads the readings from Kafka and does simple transformations to check for faulty data.
After the transformation it produces the data to another Kafka topic called sensor_cleaned. 
When the data is send two spark consumers
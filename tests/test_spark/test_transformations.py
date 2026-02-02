from src.spark_consumers.transformations import clean_sensor_data

def test_streaming_validation_professional(spark):

    output = {"valid": [], "invalid": []}

    def test_foreach_batch(df, batch_id):
        clean_sensor_data(df, batch_id, output)

    
    batch_0 = spark.createDataFrame(
        [
            (1, "2026-01-30T21:00:00", 21.6, 80.2, 1500),
        ],
        ["sensor_id", "timestamp", "temperature", "humidity", "co2"]
    )

    test_foreach_batch(batch_0, 0)

    
    batch_1 = spark.createDataFrame(
        [
            (2, "2026-01-30T21:01:00", -5.0, 50.0, 400),
            (3, "2026-01-30T21:01:10", 22.0, 40.0, 420),
        ],
        ["sensor_id", "timestamp", "temperature", "humidity", "co2"]
    )

    test_foreach_batch(batch_1, 1)

    batch_2 = spark.createDataFrame(
        [
            (None, "2026-01-30T21:01:00", -5.0, 50.0, 400),
            (3, "2026-01-321:01:10", 22.0, 40.0, 420),
        ],
        ["sensor_id", "timestamp", "temperature", "humidity", "co2"]
    )

    test_foreach_batch(batch_2, 2)


    assert output["valid"] == [1, 2, 0]
    assert output["invalid"] == [0, 0, 2]
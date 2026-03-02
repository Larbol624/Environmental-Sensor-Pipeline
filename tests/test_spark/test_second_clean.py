from src.spark_consumers.second_transform import second_transform

def test_second_clean(spark):
    Valid=spark.createDataFrame([{
        "sensor_id": 1,
        "timestamp": '2026-01-30T21:00:00.00',
        "temperature": 21.0,
        "humidity": 40.0,
        "co2":1233
    },
    {
        "sensor_id": 1,
        "timestamp": '2026-01-30T21:00:00.01',
        "temperature": 21.1,
        "humidity": 39.9,
        "co2":1250
    }])

    df=second_transform(Valid)

    rows=df.collect()


    assert len(rows) == 2

    assert rows[1]["co2_delta"]==17
    assert rows[1]["temp_delta"]==0.1
    assert rows[1]["humid_delta"]==0.1

    assert rows[1]["anomaly"]==False


    anomalies=spark.createDataFrame([{  
        "sensor_id": 1,
        "timestamp": '2026-01-30T21:00:00.00',
        "temperature": 21.0,
        "humidity": 40.0,
        "co2":1233
    },
    {
        "sensor_id": 1,
        "timestamp": '2026-01-30T21:00:00.01',
        "temperature": 28.0,
        "humidity": 60.0,
        "co2":2000
    }
    ])

    anomaly=second_transform(anomalies)

    rows=anomaly.collect()

    assert len(rows) == 2

    assert rows[1]["co2_delta"]==767
    assert rows[1]["temp_delta"]==7
    assert rows[1]["humid_delta"]==20

    assert rows[1]["anomaly"]       ==True
    assert rows[1]["temp_anomaly"]  ==True
    assert rows[1]["humid_anomaly"] ==True
    assert rows[1]["co2_anomaly"]   ==True
from src.spark_consumers.first_transform import first_transform 

def test_first_clean(spark):

    timestamp_difference=spark.createDataFrame([{
        "sensor_id":1,
        "timestamp": '2026-03-06T18:10:20.520382',
        "temperature": 21.0,
        "humidity": 50.0,
        "co2":500
    }])

    dlq,valid=first_transform(timestamp_difference)

    assert dlq.count() == 0
    assert valid.count() == 1

    
    df=spark.createDataFrame([
        {"sensor_id": 88, 
         "timestamp": "2026-03-06T18:32:17.692773", 
         "temperature": 18.58, 
         "humidity": 50.52, 
         "co2": 481}
    ])

    dlq,valid=first_transform(df)

    assert dlq.count() == 0
    assert valid.count() ==1
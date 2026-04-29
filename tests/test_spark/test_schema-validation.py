from src.spark_consumers.transforms.first_transform import first_transform 
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, DoubleType, TimestampType, StringType
)

def test_schema(spark):

    valid_schema={
        "sensor_id": 1,
        "timestamp": '2026-01-30T21:00:00',
        "temperature": 21.0,
        "humidity": 50.0,
        "co2":500
    }

    valid_df=spark.createDataFrame([valid_schema])
    dlq,valid=first_transform(valid_df)

    
    assert valid.columns == ["co2","humidity","sensor_id","temperature","timestamp","dlq_reason"]
    

    invalid_schema={
        "sensor_d": 1,
        "timestamp": '2026-01-30T21:00:00',
        "temperature": 21.0,
        "humidity": 50.0,
        "co2":500
    }

    invalid_df=spark.createDataFrame([invalid_schema])
    dlq,valid=first_transform(invalid_df)

    assert not dlq.rdd.isEmpty()
    assert valid.rdd.isEmpty()


    invalid_df2=spark.createDataFrame(
        [(
            "a",
            "21",
            "123124124",
            ""
        )],
        ["aslfjasf","lasdjsald","sladhaksjfh","askdhasf"]
    )

    dlq,valid=first_transform(invalid_df2)

    assert not dlq.rdd.isEmpty()
    assert valid.rdd.isEmpty()


    missing_values_df=spark.createDataFrame([
        {
            "sensor_id":None,
            "timestamp": '2026-01-30T21:00:00',
            "temperature": 21.0,
            "humidity": 50.0,
            "co2":500
        },
        {
            "sensor_id":1,
            "timestamp": None,
            "temperature": 21.0,
            "humidity": 50.0,
            "co2":500
        },
        {
            "sensor_id":None,
            "timestamp": None,
            "temperature": 21.0,
            "humidity": 50.0,
            "co2":500
        },
        {
            "sensor_id":1,
            "timestamp": '2026-01-30T21:00:00',
            "temperature": None,
            "humidity": None,
            "co2":None
        }
    ])

    dlq,valid=first_transform(missing_values_df)

    assert dlq.count() == 3
    assert valid.count() == 1


    wrong_types_df=spark.createDataFrame([
        {
            "sensor_id":"a",
            "timestamp": '2026-01-30T21:00:00',
            "temperature": 21.0,
            "humidity": 40.0,
            "co2":1233
        },
        {
            "sensor_id":1,
            "timestamp": '2026-01-30',
            "temperature": 21.0,
            "humidity": 40.0,
            "co2":1233
        },
        {
            "sensor_id":1,
            "timestamp": 2,
            "temperature": 21.0,
            "humidity": 40.0,
            "co2":1233
        },
        {
            "sensor_id":1,
            "timestamp": '2026-01-30T21:00:00.000000',
            "temperature": 21.0,
            "humidity": 40.0,
            "co2":1233
        },
        {
            "sensor_id":1,
            "timestamp": '2026-01-30T21:00:00.00',
            "temperature": "21312",
            "humidity": 40.0,
            "co2":1233
        }
    ])

    dlq,valid=first_transform(wrong_types_df)

    assert dlq.count() == 4

    assert valid.count() == 1


    ranges_df=spark.createDataFrame([
        {
            "sensor_id":1,
            "timestamp": '2026-03-06T18:32:17.692773',
            "temperature": 25,
            "humidity": 40.0,
            "co2":1233
        },
        {
            "sensor_id":1,
            "timestamp": '2026-01-30T21:00:00.00',
            "temperature": 125,
            "humidity": 40.0,
            "co2":1233
        },
        {
            "sensor_id":1,
            "timestamp": '2026-01-30T21:00:00.00',
            "temperature": 25,
            "humidity": 500000.0,
            "co2":-10
        }
    ])

    dlq,valid=first_transform(ranges_df)

    assert dlq.count()      == 2
    assert valid.count()    == 1
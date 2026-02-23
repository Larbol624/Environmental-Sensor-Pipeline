from src.spark_consumers.alerts import alert_check
from pyspark.sql.types import StructField, StructType, IntegerType,DoubleType
def test_alert(spark):

    schema = StructType([
    StructField("sensor_id", IntegerType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("co2", IntegerType(), True),
])
    
    no_alert=spark.createDataFrame([{
        "sensor_id":1,
        "temperature":20.0,
        "humidity":50.0,
        "co2":200
    }],schema)

    df_no_alert=alert_check(no_alert)
    
    assert df_no_alert.rdd.isEmpty()


    alert=spark.createDataFrame([{
        "sensor_id":1,
        "temperature":300.0,
        "humidity":50.0,
        "co2":200
    }],schema)

    df_alert=alert_check(alert)
    rows=df_alert.collect()
    
    assert len(rows) == 1
    assert rows[0]["alert_reasons"] == ["temperature too high"]
    


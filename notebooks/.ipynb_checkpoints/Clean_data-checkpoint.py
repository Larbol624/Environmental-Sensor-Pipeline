from pyspark.sql import SparkSession
from pyspark.sql.functions import col
def clean_data():
    test_data=[{
            "sensor_id": 2,
            "timestamp": datetime.now().isoformat(),
            "temperature": 20.0,
            "humidity": 40.0,
            "co2": 500
        }]
    
    df=spark.createDataFrame(test_data)
    df.show()
                             
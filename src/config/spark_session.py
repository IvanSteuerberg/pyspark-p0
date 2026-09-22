from pyspark.sql import SparkSession

def create_spark_session(app_name: str = "IBEX35_P0") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
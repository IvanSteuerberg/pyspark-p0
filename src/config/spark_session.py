import os
from pyspark.sql import SparkSession

def create_spark_session(app_name: str = "IBEX35_P0") -> SparkSession:
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    jar_path = os.path.join(base_dir, "drivers", "mysql-connector-j-8.3.0.jar")

    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.driver.extraClassPath", jar_path)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
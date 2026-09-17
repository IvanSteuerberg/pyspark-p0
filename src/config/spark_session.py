import os
from pyspark.sql import SparkSession

def get_spark_session(app_name: str = "IBEX35", jar_path: str = None) -> SparkSession:
    builder = SparkSession.builder.appName(app_name) #
    
    # Añadir conector JDBC si se proporciona la ruta
    if jar_path and os.path.exists(jar_path):
        builder = builder.config("spark.driver.extraClassPath", os.path.abspath(jar_path)) #
        
    return builder.getOrCreate()
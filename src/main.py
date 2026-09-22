import os
from pyspark.sql.functions import col, to_date
from config import spark_session
from models.ibex_processor import IbexProcessor


# Init
spark = spark_session.create_spark_session("IBEX35_P0")
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
csv_path = os.path.join(base_dir, "data", "ibex35_close-2024.csv")
processor = IbexProcessor(spark, csv_path)

# Ex. 1a
print("Ex. 1a:")
processor.load_data()
processor.df.printSchema()
processor.cast_date_column()
processor.df.printSchema()
processor.df.show(6)

# Ex. 1b
print("Ex. 1b:")
processor.rename_cols_without_MC()
processor.df.show(6)    # print(processor.df.head(6)) 


# Ex. 1c
print("Ex. 1c:")
processor.load_data_with_schema()
processor.df.printSchema()
processor.df.show(6)
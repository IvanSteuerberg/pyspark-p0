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

# Ex. 2a
print("Ex. 2a:")
initial_rows, final_rows = processor.drop_nulls_and_duplicates()
print(f"Initial rows: {initial_rows}, Final rows: {final_rows}")
num_companies = processor.get_num_companies()
print(f"Number of companies: {num_companies}")

# Ex. 2b:
print("Ex. 2b:")
start_date, end_date, num_days = processor.get_date_range_and_num_days()
# total_days = (end_date - start_date).days + 1 
# Son 364 días en total y tenemos 255 días con información, pero la bolsa no abre todos los días, por lo que el número de días con información es menor.
# El resultado es coherente con lo esperado. No considero necesario buscar datos adicionales para completar el periodo.
print(f"Start date: {start_date}, End date: {end_date}, Number of days: {num_days}")

# Ex. 3a:
print("Ex. 3a:")
processor.rename_fecha_to_dia()
processor.df.show(10)
annual_stats = processor.calculate_annual_stats()
print("Annual statistics:")
annual_stats.show(1, vertical=True)
processor.add_deficiency_notice_column()
processor.df.show(100)  # Show 100 rows of the DataFrame

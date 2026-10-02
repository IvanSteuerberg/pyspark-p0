import os
from pyspark.sql.functions import col, to_date
from config import spark_session
from models.ibex_processor import IbexProcessor


# Init
spark = spark_session.create_spark_session("IBEX35_P0")
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
csv_path = os.path.join(base_dir, "data", "ibex35_close-2024.csv")
processor = IbexProcessor(spark, csv_path)

# Ej1-a
print("Ej1-a")
processor.load_data()
processor.df.printSchema()
processor.cast_date_column()
processor.df.printSchema()
processor.df.show(6)

# Ej1-b
print("Ej1-b")
processor.rename_cols_without_MC()
processor.df.show(6)    # print(processor.df.head(6)) 

# Ej1-c
print("Ej1-c")
processor.load_data_with_schema()
processor.df.printSchema()
processor.df.show(6)

# Ej2-a
print("Ej2-a")
initial_rows, final_rows = processor.drop_nulls_and_duplicates()
print(f"Filas eliminadas: {initial_rows - final_rows}")
num_companies = processor.get_num_companies()
print(f"Número de empresas: {num_companies}")

# Ej2-b
print("Ej2-b")
start_date, end_date, num_days = processor.get_date_range_and_num_days()
print(f"Fecha inicial: {start_date}, Fecha final: {end_date}, Días disponibles: {num_days}")
print("Son 364 días en total y tenemos 255 días con información, pero la bolsa no abre todos los días, por lo que el número de días con información es menor.\n" \
    "El resultado es coherente con lo esperado. No considero necesario buscar datos adicionales para completar el periodo.")

# Ej3
print("Ej3")
processor.rename_fecha_to_dia()
processor.df.show(10)
annual_stats = processor.calculate_annual_stats()
print("Estadísticas anuales:")
annual_stats.show(1, vertical=True)
processor.add_deficiency_notice_column()
processor.df.show(100)  # Show 100 rows of the DataFrame

# Ej3-b
print("Ej3-b")
print("Son empresas del IBEX35. Los nulos corresponden a empresas que salieron a bolsa a mediados de año y los que salieron del índice a mitad de año. " \
"\nNo afectan a los cálculos de media, máximo y mínimo porque las funciones de agregación de PySpark (avg, max, min) ignoran los nulos." \
"\nFuente: datosmacro.com, https://datosmacro.expansion.com/bolsa/espana")

# Ej4
print("Ej4")
variation_df = processor.calculate_annual_variation()
print("DataFrame de variación:")
variation_df.show(40, truncate=False)  # Show 40 rows of the variation DataFrame

# Ej5
print("Ej5")
processor.add_quartile_columns()
print("Primera fila del DataFrame con cuartiles:")
processor.df.show(1)
print("Columnas de AENA y BBVA con sus cuartiles:")
processor.df.select("Dia", "AENA", "AENACuartil", 
"BBVA", "BBVACuartil").orderBy("Dia").show(processor.df.count(), truncate=False)

# Ej6 (Uso de IA: Gemini como asistente de búsqueda bibliográfica para localizar las noticias de Grifols y Sabadell)
print("Ej6")
processor.add_significant_change_columns()

print("Fila 15 del DataFrame:")
fila_15 = processor.df.orderBy("Dia").head(15)[14]
spark.createDataFrame([fila_15], schema=processor.df.schema).show(vertical=True)

print("En un día normal como el de la fila 15 ninguna acción se mueve tanto. Los cambios de más del 8% en el año se deben a noticias imprevistas, ",\
"principalmente la caída de Grifols en enero por el informe de Gotham y la subida de Sabadell en abril tras presentar resultados récord.")
print("Fuentes: \n"
    "https://www.gothamcityresearch.com/ , https://elpais.com/economia/2024-01-09/grifols-se-hunde-en-bolsa-tras-un-informe-de-la-firma-de-analisis-que-destapo-el-fraude-de-gowex.html",
    "https://www.elconfidencial.com/empresas/2024-04-25/sabadell-resultados-beneficio-margenes_3872416/")

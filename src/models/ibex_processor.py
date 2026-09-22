from pyspark.sql.functions import col, to_date
from pyspark.sql.types import DoubleType, StructType, StructField, DateType

class IbexProcessor:
    def __init__(self, spark_session, csv_path):
        self.spark = spark_session
        self.csv_path = csv_path
        self.df = None

    def load_data(self):
        self.df = (
            self.spark.read
            .option("header", True)
            .option("sep", ";")
            .csv(self.csv_path)
        )
        return self.df

    def cast_date_column(self):
        # data_frame = data_frame.withColumn("start_date", col("start_date").cast("date"))
        self.df = self.df.withColumn("Fecha", to_date(col("Fecha"), "dd/MM/yyyy"))
        return self.df

    def rename_cols_without_MC(self):
        # data_frame.withColumnRenamed(columna_existente, nueva_columna)
        for col_name in self.df.columns:
            if col_name.endswith(".MC"):
                new_name = col_name.replace(".MC", "")
                self.df = self.df.withColumnRenamed(col_name, new_name)
        return self.df

    def load_data_with_schema(self):
        schema = StructType([
                StructField("Fecha", DateType(), True),
            StructField("IBE", DoubleType(), True),
            StructField("REP", DoubleType(), True),
            StructField("NTGY", DoubleType(), True),
            StructField("ELE", DoubleType(), True),
            StructField("ENG", DoubleType(), True),
            StructField("RED", DoubleType(), True),
            StructField("SAN", DoubleType(), True),
            StructField("BBVA", DoubleType(), True),
            StructField("CABK", DoubleType(), True),
            StructField("BKT", DoubleType(), True),
            StructField("SAB", DoubleType(), True),
            StructField("UNI", DoubleType(), True),
            StructField("MAP", DoubleType(), True),
            StructField("ACS", DoubleType(), True),
            StructField("ANA", DoubleType(), True),
            StructField("ANE", DoubleType(), True),
            StructField("ACX", DoubleType(), True),
            StructField("MTS", DoubleType(), True),
            StructField("SCYR", DoubleType(), True),
            StructField("CLNX", DoubleType(), True),
            StructField("TEF", DoubleType(), True),
            StructField("AENA", DoubleType(), True),
            StructField("FER", DoubleType(), True),
            StructField("ITX", DoubleType(), True),
            StructField("AMS", DoubleType(), True),
            StructField("IAG", DoubleType(), True),
            StructField("GRF", DoubleType(), True),
            StructField("FDR", DoubleType(), True),
            StructField("SLR", DoubleType(), True),
            StructField("ROVI", DoubleType(), True),
            StructField("LOG", DoubleType(), True),
            StructField("IDR", DoubleType(), True),
            StructField("MEL", DoubleType(), True),
            StructField("PUIG", DoubleType(), True),
            StructField("COL", DoubleType(), True),
            StructField("MRL", DoubleType(), True),
        ])
        self.df = (
            self.spark.read
            .option("header", True)
            .option("sep", ";")
            .option("dateFormat", "dd/MM/yyyy")
            .schema(schema)
            .csv(self.csv_path)
        )
        return self.df
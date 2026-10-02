from pyspark.sql.functions import col, first, last, to_date, avg, max, min, year, when, lag, abs, round as spark_round
from pyspark.sql.types import DoubleType, StructType, StructField, DateType
from pyspark.sql.window import Window

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

    def drop_nulls_and_duplicates(self):
        initial_rows = self.df.count()
        self.df = self.df.dropna(how="all")
        self.df = self.df.dropDuplicates()
        final_rows = self.df.count()
        return initial_rows, final_rows

    def get_num_companies(self):
        num_companies = len(self.df.columns) - 1  # Subtract 1 for the date column
        return num_companies

    def get_date_range_and_num_days(self):
        start_date = self.df.agg({"Fecha": "min"}).collect()[0][0]
        end_date = self.df.agg({"Fecha": "max"}).collect()[0][0]
        num_days = self.df.select("Fecha").distinct().count()
        return start_date, end_date, num_days

    def rename_fecha_to_dia(self):
        self.df = self.df.withColumnRenamed("Fecha", "Dia")
        return self.df

    def calculate_annual_stats(self):
        # Extract the year from the "Dia" column
        self.df = self.df.withColumn("Year", year(col("Dia")))

        # Calculate annual statistics for each company
        annual_stats = (
            self.df.groupBy("Year")
            .agg(
                *[
                    avg(col(company)).alias(f"{company}_mean") for company in self.df.columns if company != "Dia" and company != "Year"
                ],
                *[
                    max(col(company)).alias(f"{company}_max") for company in self.df.columns if company != "Dia" and company != "Year"
                ],
                *[
                    min(col(company)).alias(f"{company}_min") for company in self.df.columns if company != "Dia" and company != "Year"
                ]
            )
        )
        return annual_stats

    def add_deficiency_notice_column(self):
        print("Adding 'Deficiency Notice UNI' column...")
        self.df = self.df.withColumn("Deficiency Notice UNI", col("UNI") < 1)

    def calculate_annual_variation(self):

        # Order the df
        df_sorted = self.df.orderBy("Dia")

        # Filter
        excluded_cols = {"Dia", "Year", "Deficiency Notice UNI", "Fecha"}
        companies = [c for c in self.df.columns if c not in excluded_cols]

        # Agg
        agg_exprs = []
        for c in companies:
            agg_exprs.append(first(col(c), ignorenulls=True).alias(f"{c}_inicial"))
            agg_exprs.append(last(col(c), ignorenulls=True).alias(f"{c}_final"))

        min_max_values = df_sorted.agg(*agg_exprs).collect()[0].asDict()

        data = []
        for c in companies:
            initial = min_max_values.get(f"{c}_inicial")
            final = min_max_values.get(f"{c}_final")

            if initial is not None and final is not None and initial != 0:
                variation = ((final - initial) / initial) * 100

                if variation >= 15:
                    clasification = "Subida Fuerte"
                elif variation > 1:
                    clasification = "Subida"
                elif variation >= -1:
                    clasification = "Neutra"
                elif variation > -15:
                    clasification = "Bajada"
                else:
                    clasification = "Bajada Fuerte"

                data.append((c, round(initial, 4), round(final, 4), round(variation, 2), clasification))

        schema = ["Empresa", "Valor_Inicial", "Valor_Final", "Variacion_Porcentual", "Clasificacion"]
        return self.spark.createDataFrame(data, schema=schema)
    
    def add_quartile_columns(self):
        
        excluded_cols = {"Dia", "Year", "Deficiency Notice UNI", "Fecha"}
        companies = [c for c in self.df.columns if c not in excluded_cols]
        
        for c in companies:
            q = self.df.approxQuantile(c, [0.25, 0.5, 0.75], 0.01)
            q1, q2, q3 = q[0], q[1], q[2]
            
            col_name = f"{c}Cuartil"
            self.df = self.df.withColumn(
                col_name, 
                when(col(c).isNull(), None)
                .when(col(c) <= q1, "q1")
                .when(col(c) <= q2, "q2")
                .when(col(c) <= q3, "q3")
                .otherwise("q4")
            )
        return self.df

    def add_significant_change_columns(self):
        wind = Window.orderBy("Dia")

        excluded_cols = {"Dia", "Year", "Deficiency Notice UNI", "Fecha"}
        companies = [c for c in self.df.columns if c not in excluded_cols and not c.endswith("Cuartil")]

        for c in companies:
            prev_price = lag(col(c), 1).over(wind)
            var_pct = ((col(c) - prev_price) / prev_price) * 100
            col_name = f"{c}CambioSignificativo"
            self.df = self.df.withColumn(col_name, when(abs(var_pct) > 8, spark_round(var_pct, 2).cast("string"))
            .otherwise("-"))
        
        return self.df

            
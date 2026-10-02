class DBConnector:
    def __init__(self, spark_session, host="localhost", port=3306, db_name="IBEX35", user="root", password="root"):
        self.spark = spark_session
        self.host = host
        self.port = port
        self.db_name = db_name
        self.user = user
        self.password = password
        self.url = f"jdbc:mysql://{host}:{port}/{db_name}"
        self.properties = {
            "driver": "com.mysql.cj.jdbc.Driver",
            "user": user,
            "password": password
        }
        self._ensure_database_exists()

    def _ensure_database_exists(self):
        """Crea la base de datos si aún no existe en el servidor MySQL."""
        try:
            conn = self.spark._jvm.java.sql.DriverManager.getConnection(
                f"jdbc:mysql://{self.host}:{self.port}/?allowPublicKeyRetrieval=true&useSSL=false",
                self.user,
                self.password
            )
            stmt = conn.createStatement()
            stmt.execute(f"CREATE DATABASE IF NOT EXISTS {self.db_name}")
            stmt.close()
            conn.close()
        except Exception:
            pass

    def write_table(self, df, table_name, mode="overwrite"):
        """Escribe un DataFrame de PySpark en la base de datos MySQL mediante JDBC."""
        df.write.jdbc(
            url=self.url,
            table=table_name,
            mode=mode,
            properties=self.properties
        )

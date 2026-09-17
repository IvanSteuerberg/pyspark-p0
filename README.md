# IBEX-35 Data Processing with PySpark

This repository contains the solution for Practice 0 of the Data Mining course. The project focuses on data preprocessing, feature transformation, and exploratory queries on historical IBEX-35 stock data using PySpark. The final results are then exported to a MySQL database via JDBC.

## Project Structure

The project follows the MVC (Model-View-Controller) pattern and uses relative paths:

```text
.
├── data/
│   └── ibex35.csv
├── jars/
│   └── mysql-connector-j-8.0.33.jar
├── src/
│   ├── config/
│   │   ├── spark_session.py
│   │   └── database.py
│   ├── controllers/
│   │   └── ibex_controller.py
│   ├── models/
│   │   └── ibex_model.py
│   └── views/
│       └── console_view.py
├── main.py
├── .gitignore
├── README.md
└── requirements.txt (optional if used)
```

## Environment Setup

### 1. Requirements

- Python 3.10
- PySpark 3.3
- OpenJDK 17
- openpyxl 3.1.5
- Graphviz

### 2. Conda Environment

```bash
conda create --name mineriadedatos2026 python=3.10 -y
conda activate mineriadedatos2026
conda install -c anaconda graphviz -y
conda install -c conda-forge pyspark=3.3 -y
conda install -c conda-forge openjdk=17 -y
conda install -c anaconda openpyxl=3.1.5 -y
```

### 3. Windows Environment Variables (inside Conda)

To ensure compatibility between PySpark 3.3 and Java 17, set the following environment variables:

```bash
conda env config vars set PYSPARK_PYTHON="%CONDA_PREFIX%\python.exe"
conda env config vars set PYSPARK_DRIVER_PYTHON="%CONDA_PREFIX%\python.exe"
conda env config vars set JDK_JAVA_OPTIONS="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"

conda deactivate
conda activate mineriadedatos2026
```

## Running the Application

Make sure your local MySQL instance is running and that the database `IBEX35` exists, then execute:

```bash
python main.py
```

This project is intended to demonstrate the workflow of processing financial data with Spark, transforming it into useful features, and persisting the results in a relational database for further analysis.
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, upper, trim, col
import pyodbc


# --- Caminho do driver JDBC ---
PROJECT_DIR = "/home/leandro/Documentos/Workspace/sql-datawarehouse-project"
JDBC_DRIVER = f"{PROJECT_DIR}/lib/mssql-jdbc-12.4.2.jre11.jar"

# --- Configurar Spark ---
spark = SparkSession.builder \
        .appName("Silver - Transform crm_cust_info") \
        .config("spark.jars", JDBC_DRIVER) \
        .config("spark.driver.extraClassPath", JDBC_DRIVER) \
        .config("spark.executor.extraClassPath", JDBC_DRIVER) \
        .getOrCreate()

# --- Configuração do SQL Server ---
jdbc_url = "jdbc:sqlserver://localhost:1433;databaseName=DataWarehouse;encrypt=false"
connection_properties = {
    "user": "sa",
    "password": "123456Lo",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# --- Tabela bronze do SQL Server ---
bronze_df_crm_cust_info = spark.read.jdbc(
    url= jdbc_url,
    table="bronze.crm_cust_info",
    properties=connection_properties
)

silver_df_crm_cust_info = bronze_df_crm_cust_info \
    .withColumn('cst_firstname', trim(upper(col('cst_firstname')))) \
    .withColumn('cst_lastname', trim(upper(col('cst_lastname')))) \
    .withColumn('cst_marital_status', 
                when(
                    col('cst_marital_status') == 'M', "Married")
                .otherwise("Single")) \
    .withColumn('cst_gndr', 
                when(
                    col('cst_gndr') == 'M', 'Male'
                ).otherwise('Female')) \
    .filter(col('cst_id').isNotNull())

silver_df_crm_cust_info.write.jdbc(
    url= jdbc_url,
    table="silver.crm_cust_info",
    mode="overwrite",
    properties=connection_properties
)

spark.stop()
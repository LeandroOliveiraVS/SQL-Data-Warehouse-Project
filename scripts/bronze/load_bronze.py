from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

# --- Variaveis do Ambiente ---
load_dotenv()

PROJECT_DIR = os.getenv('PROJECT_DIR')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# --- Caminho do driver JDBC ---
JDBC_DRIVER = f"{PROJECT_DIR}/lib/mssql-jdbc-12.4.2.jre11.jar"

# --- Configurar o Spark ---
spark = SparkSession.builder \
            .appName("Bronze - Load") \
            .config("spark.jars", JDBC_DRIVER) \
            .config("spark.driver.extraClassPath", JDBC_DRIVER) \
            .config("spark.executor.extraClassPath", JDBC_DRIVER) \
            .getOrCreate()

# --- Configuração do SQL Server ---
jdbc_url = "jdbc:sqlserver://localhost:1433;databaseName=DataWarehouse;encrypt=false"
connection_properties = {
    "user": "sa",
    "password": DB_PASSWORD,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# -- Arquivos CSV --
crm_cust_info = spark.read.csv(f"{PROJECT_DIR}/datasets/source_crm/cust_info.csv", header=True)

crm_prd_info = spark.read.csv(f"{PROJECT_DIR}/datasets/source_crm/prd_info.csv", header=True)

crm_sales_details = spark.read.csv(f"{PROJECT_DIR}/datasets/source_crm/sales_details.csv", header=True)

erp_cust_az12 = spark.read.csv(f"{PROJECT_DIR}/datasets/source_erp/CUST_AZ12.csv", header=True)

erp_loc_a101 = spark.read.csv(f"{PROJECT_DIR}/datasets/source_erp/LOC_A101.csv", header=True)

erp_px_cat_g1v2 = spark.read.csv(f"{PROJECT_DIR}/datasets/source_erp/PX_CAT_G1V2.csv", header=True)

tables_to_load = {
    "crm_cust_info":    crm_cust_info,
    "crm_prd_info":     crm_prd_info,
    "crm_sales_details": crm_sales_details,
    "erp_cust_az12":    erp_cust_az12,
    "erp_loc_a101":     erp_loc_a101,
    "erp_px_cat_g1v2":  erp_px_cat_g1v2
}

for table_name, table in tables_to_load.items():
    full_table_name = f"bronze.{table_name}"

    table.write.jdbc(
        url= jdbc_url,
        table=full_table_name,
        mode="overwrite",
        properties=connection_properties
    )

spark.stop()
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, upper, trim, col, try_to_date, current_date, initcap, concat, lit, regexp_replace
from dotenv import load_dotenv
import os

# -- Variaveis do ambiente --
load_dotenv()

PROJECT_DIR = os.getenv('PROJECT_DIR')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# --- Caminho do driver JDBC ---
JDBC_DRIVER = f"{PROJECT_DIR}/lib/mssql-jdbc-12.4.2.jre11.jar"

# --- Configurar Spark ---
spark = SparkSession.builder \
        .appName("Silver - Transform & Load") \
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

# --- Tabelas ---

# -- Tabela Silver crm_cust_info --
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

# -- Tabela Silver crm_prd_info --
bronze_df_crm_prd_info = spark.read.jdbc(
    url= jdbc_url,
    table="bronze.crm_prd_info",
    properties=connection_properties
)

silver_df_crm_prd_info = bronze_df_crm_prd_info \
    .withColumn('prd_nm', trim(upper(col('prd_nm')))) \
    .withColumn('prd_key', trim(upper(col('prd_key')))) \
    .withColumn('prd_line', 
        when(trim(upper(col('prd_line')))== 'M', 'Mountain').
        when(trim(upper(col('prd_line'))) == 'R', 'Road').
        when(trim(upper(col('prd_line'))) == 'S', 'Other Sales').
        when(trim(upper(col('prd_line'))) == 'T', 'Touring').otherwise('N/A')
    ) \
    .withColumn('prd_start_dt', try_to_date(col('prd_start_dt'))) \
    .withColumn('prd_end_dt', try_to_date(col('prd_end_dt'))) \
    

silver_df_crm_prd_info.write.jdbc(
    url= jdbc_url,
    table="silver.crm_prd_info",
    mode="overwrite",
    properties=connection_properties
)

# -- Tabela Silver crm_sales_details -- 
bronze_df_crm_sales_details = spark.read.jdbc(
    url= jdbc_url,
    table="bronze.crm_sales_details",
    properties=connection_properties
)

silver_crm_sales_details = bronze_df_crm_sales_details \
    .withColumn('sls_ord_num', trim(upper(col('sls_ord_num')))) \
    .withColumn('sls_prd_key', trim(upper(col('sls_ord_num')))) \
    .withColumn('sls_order_dt', 
        try_to_date(col('sls_order_dt').cast('string'), 'yyyyMMdd')
    ) \
    .withColumn('sls_ship_dt',
        try_to_date(col('sls_ship_dt').cast('string'), 'yyyyMMdd')
    ) \
    .withColumn('sls_due_dt',
        try_to_date(col('sls_due_dt').cast('string'), 'yyyyMMdd')
    )

silver_crm_sales_details.write.jdbc(
    url= jdbc_url,
    table="silver.crm_sales_details",
    mode="overwrite",
    properties=connection_properties
)

# -- Silver erp_cust_az12 --
bronze_df_erp_cust_az12 = spark.read.jdbc(
    url= jdbc_url,
    table="bronze.erp_cust_az12",
    properties=connection_properties
)

silver_df_erp_cust_az12 = bronze_df_erp_cust_az12 \
    .withColumn('cid', trim(upper(regexp_replace(col('cid'), r'^NAS', '')))) \
    .withColumn('dwh_create_date', current_date()) \
    .withColumn('gen', 
        when(trim(upper(col('gen'))).isin('M', 'MALE'), 'Male')
        .when(trim(upper(col('gen'))).isin('F', 'FEMALE'), 'Female')
        .otherwise('N/A')
    ) \
    .withColumn('cid', trim(upper(col('cid'))))

silver_df_erp_cust_az12.write.jdbc(
    url= jdbc_url,
    table="silver.erp_cust_az12",
    mode="overwrite",
    properties=connection_properties
)

# -- Silver erp_loc_a101 --
bronze_df_erp_loc_a101 = spark.read.jdbc(
    url= jdbc_url,
    table="bronze.erp_loc_a101",
    properties=connection_properties
)

silver_df_erp_loc_a101 = bronze_df_erp_loc_a101 \
    .withColumn('cid', trim(upper(regexp_replace(col('cid'), r'-', '')))) \
    .withColumn('dwh_create_date', current_date()) \
    .withColumn('cntry', 
        when(trim(upper(col('cntry'))) == 'DE', 'Germany').
        when(trim(upper(col('cntry'))).isin('US', 'USA'), 'United States').
        when(trim(upper(col('cntry'))).isNull(), 'N/A').
        otherwise(trim(initcap(col('cntry'))))
    )

silver_df_erp_loc_a101.write.jdbc(
    url= jdbc_url,
    table="silver.erp_loc_a101",
    mode="overwrite",
    properties=connection_properties
)

# -- Silver erp_px_cat_g1v2 --
bronze_df_erp_px_cat_g1v2 = spark.read.jdbc(
    url= jdbc_url,
    table="bronze.erp_px_cat_g1v2",
    properties=connection_properties
)

silver_df_erp_px_cat_g1v2 = bronze_df_erp_px_cat_g1v2 \
    .withColumn('id', trim(upper(col('id')))) \
    .withColumn('cat', trim(initcap(col('cat')))) \
    .withColumn('subcat', trim(initcap(col('subcat')))) \
    .withColumn('maintenance', trim(initcap(col('maintenance')))) \

silver_df_erp_px_cat_g1v2.write.jdbc(
    url= jdbc_url,
    table="silver.erp_px_cat_g1v2",
    mode="overwrite",
    properties=connection_properties
)

spark.stop()
USE DataWarehouse;
GO

-- =============================================================================
-- Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
    ci.cst_id               AS customer_id,
    ci.cst_key              AS customer_number,
    ci.cst_firstname        AS firstname,
    ci.cst_lastname         AS lastname,
    ci.cst_marital_status   AS marital_status,
    CASE 
        WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr -- CRM is the primary source for gender
        ELSE COALESCE(ca.gen, 'N/A')  			   -- Fallback to ERP data
    END                     AS gender,             
    ca.BDATE                AS birthday,
    cl.cntry                AS country
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS cl
    ON ci.cst_key = cl.cid;
GO

-- =============================================================================
-- Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO



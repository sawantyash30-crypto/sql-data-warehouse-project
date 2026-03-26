
EXEC bronze.load_bronze
EXEC silver.load_silver
 

SELECT * 
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME = 'crm_sales_details';

SELECT * FROM bronze.crm_sales_details;
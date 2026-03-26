
use datawarehouse;
if OBJECT_ID (' silver.crm_cust_info','U')	IS NOT NULL
DROP TABLE silver.crm_cust_info;
create table silver.crm_cust_info(
	cst_id int,
	cst_key nvarchar(50),
	cst_firstname varchar(50),
	cst_lastname varchar(50),
	cst_material_status varchar(50),
	cst_gndr varchar(50),
	cst_create_date date,
	dwh_create_date datetime2 default getdate()
);

if OBJECT_ID ('silver.crm_prd_info','U')	IS NOT NULL
DROP TABLE silver.crm_prd_info;
create table silver.crm_prd_info(
	prd_id int,
	cat_id nvarchar(50),
	prd_key nvarchar(50),
	prd_nm nvarchar(50),
	prd_cost int,
	prd_line nvarchar(20),
	prd_start_dt date,
	prd_end_dt date,
	dwh_create_date datetime2 default getdate()
);

if OBJECT_ID (' silver.crm_sales_details','U')	IS NOT NULL
DROP TABLE silver.crm_sales_details;
create table silver.crm_sales_details(
	sls_ord_num nvarchar(50),
	sls_prd_key nvarchar(50),
	sls_cust_id int,
	sls_ordr_dt date,
	sls_ship_dt date,
	sls_due_dt date,
	sls_sales int,
	sls_quantity int,
	sls_price int,
	dwh_create_date datetime2 default getdate()
);

if OBJECT_ID ('silver.erp_cust_az12','U')	IS NOT NULL
DROP TABLE silver.erp_cust_az12;
create table silver.erp_cust_az12 (
	cid nvarchar(50),
	bdate date,
	gen nvarchar(50),
	dwh_create_date datetime2 default getdate()
);

if OBJECT_ID ('silver.erp_loc_a101','U')	IS NOT NULL
DROP TABLE silver.erp_loc_a101;
create table silver.erp_loc_a101 (
	cid nvarchar(50),
	cntry nvarchar(50),
	dwh_create_date datetime2 default getdate()
);

if OBJECT_ID ('silver.erp_px_cat_g1v2','U')	IS NOT NULL
DROP TABLE silver.erp_px_cat_g1v2;
create table silver.erp_px_cat_g1v2 (
	id nvarchar(50),
	cat nvarchar (50),
	subcat nvarchar(50),
	maintainance nvarchar(50),
	dwh_create_date datetime2 default getdate()
);

truncate table silver.crm_cust_info;
bulk insert silver.crm_cust_info
from "C:\Users\admin\Downloads\datawarehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv"
with (	
	firstrow = 2,
	fieldterminator = ',',
	tablock
);

select COUNT(*) from silver.crm_cust_info;

truncate table silver.crm_prd_info;
bulk insert silver.crm_prd_info
from "C:\Users\admin\Downloads\datawarehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv"
with (
	firstrow = 2,
	fieldterminator = ',',
	tablock
);

truncate table silver.crm_sales_details;
bulk insert silver.crm_sales_details
from "C:\Users\admin\Downloads\datawarehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv"
with (
	firstrow = 2,
	fieldterminator = ',',
	tablock
);

truncate table silver.erp_cust_az12;
bulk insert silver.erp_cust_az12
from "C:\Users\admin\Downloads\datawarehouse\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv"
with (
	firstrow = 2,
	fieldterminator = ',',
	tablock
);

truncate table silver.erp_loc_a101;
bulk insert silver.erp_loc_a101
from "C:\Users\admin\Downloads\datawarehouse\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv"
with (
	firstrow = 2 ,
	fieldterminator = ',',
	tablock
);

truncate table silver.erp_px_cat_g1v2;
bulk insert silver.erp_px_cat_g1v2
from "C:\Users\admin\Downloads\datawarehouse\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv"
with (
	firstrow = 2,
	fieldterminator = ',',
	tablock 
);

select * from silver.erp_px_cat_g1v2;
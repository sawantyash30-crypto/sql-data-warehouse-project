--check for nulls or duplicates in primary key
---Expectation: No Result	
use datawarehouse

select * from bronze.crm_cust_info;

select cst_id,
COUNT(*) from bronze.crm_cust_info
group by cst_id
having COUNT(*) >1;

select * from bronze.crm_cust_info
where cst_id=29466

select *
from (
select *,
ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_last
from silver.crm_cust_info
where cst_id is not null
) t where flag_last =1

--chech for unwanted spaces
--expectation no results
select cst_gndr
from silver.crm_cust_info
where cst_gndr !=TRIM(cst_gndr)

select prd_nm
from bronze.crm_prd_info
where prd_nm !=TRIM(prd_nm)

--Data Standardization & Consistency
select distinct gen
from bronze.erp_cust_az12

select * from silver.crm_cust_info

select 
prd_id,
COUNT(*)
from bronze.crm_prd_info
group by prd_id
having COUNT(*) >1 or prd_id is  null

--check for nulls or negative numbers
--- expectation: no result
select prd_cost
from bronze.crm_prd_info
where prd_cost < 0 or prd_cost is null

--check for invalid date orders
select * 
from bronze.crm_prd_info
where prd_end_dt<prd_start_dt

select 
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt)over (partition by prd_key order by prd_start_dt)-1 as prd_end_dt_test
from bronze.crm_prd_info
where prd_key in ('AC-HE-HL-U509-R','AC-HE-HL-U509')

--check for invalid dates
select 
nullif(sls_ordr_dt,0) sls_ordr_dt
from bronze.crm_sales_details
where sls_ordr_dt <=0
or LEN(sls_ordr_dt) ! =8
or sls_ordr_dt > 20500101
or sls_ordr_dt< 19000101

-- check data consistency : between sales,quantity and price
-- sales>>=quantity*price
--- values must not be null,zero or negative

select distinct
sls_sales ,
sls_quantity,
sls_price
from silver.crm_sales_details
where sls_sales ! = sls_quantity*sls_price

select * from silver.crm_sales_details

-- identify out of range dates

select 
bdate
from silver.erp_cust_az12
where bdate<'1925-01-01' or bdate>GETDATE()

--Data Standardization & Consistency
select distinct 
case when upper(trim(gen)) in ('F','FEMALE') THEN 'Female'
     when upper(trim(gen)) in ('M','MALE') THEN 'Male'
     Else 'n/a'
end as gen
from silver.erp_cust_az12   

select * from silver.erp_cust_az12

--Data Standardization & Consistency
select distinct
cntry as old_cntry,
case when trim(cntry) ='DE' THEN 'Germany'
     when trim(cntry) in('US', 'USA') THEN 'United States'
     when trim(cntry) =''or cntry is null then 'n/a'
     else trim(cntry)
end as cntry
from bronze.erp_loc_a101
order by cntry

--check for unwanted spaces
select * from bronze.erp_px_cat_g1v2
where cat ! = TRIM(cat) or subcat ! = TRIM(subcat) or maintainance ! = TRIM(maintainance)

----Data Standardization & Consistency
select distinct
maintainance
from bronze.erp_px_cat_g1v2
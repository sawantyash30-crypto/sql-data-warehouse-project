
use datawarehouse;

create or alter procedure silver.load_silver  as
begin
  declare @start_time datetime,@end_time datetime,@batch_start_time datetime,@batch_end_time datetime;
  begin try
    set @batch_start_time=GETDATE();
    print'============================================';
	print'Loading Silver Layer';
	print'============================================';

	print'=============================================';
	print'Loading CRM Tables';
	PRINT'=============================================';
	
	set @start_time=GETDATE();
    print'>> Truncating Table: silver.crm_cust_info'
    truncate table silver.crm_cust_info
    print'>>Inserting Into Data: silver.crm_cust_info'
    insert into silver.crm_cust_info(
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_material_status,
    cst_gndr,
    cst_create_date)
    select
    cst_id,
    cst_key,
    trim(cst_firstname)as cst_first_name,
    TRIM(cst_lastname)as cst_last_name,
    case when upper(trim(cst_material_status))= 'S' THEN 'Single' 
         when UPPER(trim(cst_material_status))='M' THEN 'Married'
         else 'n/a'
    end cst_material_status,
    case when upper(trim(cst_gndr)) ='M' THEN 'Male'
         when upper(trim(cst_gndr)) = 'F'THEN 'Female'
         else 'n/a'
    end cst_gndr,
    cst_create_date
    from (
    select *,
    ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_last
    from bronze.crm_cust_info
    where cst_id is not null
    ) t where flag_last =1
    set @end_time=GETDATE();
	print'>>Load Duration:' + cast(datediff (second,@start_time ,@end_time) as nvarchar) + 'seconds';
	print'>>---------------------';
 

   	set @start_time=GETDATE();
    print'>> Truncating Table: silver.crm_prd_info'
    truncate table silver.crm_prd_info
    print'>>Inserting Into Data:silver.crm_prd_info'
    insert into silver.crm_prd_info(
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt)
    select 
        prd_id,
        REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
       SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key,
       prd_nm,
       isnull(prd_cost,0) as prd_cost,
        case UPPER(trim(prd_line))
             when 'M' THEN 'Mountain'
             when 'R' THEN 'Road'
             when 'S' THEN 'Other Sales'
             when 'T' THEN 'Touring'
             ELSE 'N/A'
        END as prd_line,
        CAST (prd_start_dt as date) as prd_start_dt,
    CAST(LEAD(prd_start_dt) over(partition by prd_key order by prd_start_dt)-1 as date) as prd_end_dt
    from bronze.crm_prd_info
    set @end_time=GETDATE();
	print'>>Load Duration:' + cast(datediff (second,@start_time ,@end_time) as nvarchar) + 'seconds';
	print'>>---------------------';
 

	set @start_time=GETDATE();
    print'>> Truncating Table: silver.crm_sales_details'
    truncate table silver.crm_sales_details
    print'>>Inserting Into Data: silver.crm_sales_details'
    insert into silver.crm_sales_details
    ( sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_ordr_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
    )
    select
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    case when sls_ordr_dt=0 Or len(sls_ordr_dt)!=8 then null
    else CAST(cast(sls_ordr_dt as varchar) AS date)
    end as sls_ordr_dt,
    case when sls_ship_dt=0 OR LEN(sls_ship_dt) ! =8 then null
    else CAST(cast(sls_ship_dt as varchar) AS date)
    end as sls_ship_dt,
    case when sls_due_dt=0 OR LEN(sls_due_dt)!=8 then null
    else CAST(cast(sls_due_dt as varchar) AS date)
    end as sls_due_dt,
    case when sls_sales is null or sls_sales <=0 or sls_sales!=sls_quantity * abs(sls_price)
         then sls_quantity* abs(sls_price)
       else sls_sales
    end as sls_sales,
    sls_quantity,
    case when sls_price is null or sls_price<=0
       then sls_sales/nullif(sls_quantity,0)
       else sls_price
    end as sls_price
    from bronze.crm_sales_details
    set @end_time=GETDATE();
	print'>>Load Duration:' + cast(datediff (second,@start_time ,@end_time) as nvarchar) + 'seconds';
	print'>>---------------------';
 

    print'=============================================';
	print'Loading ERP Tables';
	PRINT'=============================================';


	set @start_time=GETDATE();
    print'>> Truncating Table: silver.erp_cust_az12'
    truncate table silver.erp_cust_az12
    print'>> Inserting Inro Data: silver.erp_cust_az12'
    insert into silver.erp_cust_az12(cid,bdate,gen)
    select 
    case when cid like 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
         else cid
    end as cid,
    case when bdate > GETDATE() then null
    else bdate
    end as bdate,
    case when upper(trim(gen)) in ('F','FEMALE') THEN 'Female'
         when upper(trim(gen)) in ('M','MALE') THEN 'Male'
         Else 'n/a'
    end as gen
    from bronze.erp_cust_az12

	set @start_time=GETDATE();
    print'>> Truncating Table: silver.erp_loc_a101'
    truncate table silver.erp_loc_a101
    print'>> Inserting Inro Data: silver.erp_loc_a101'
    insert into silver.erp_loc_a101(cid,cntry)
    select
    REPLACE(cid,'-','') cid,
    case when trim(cntry) ='DE' THEN 'Germany'
         when trim(cntry) in('US', 'USA') THEN 'United States'
         when trim(cntry) =''or cntry is null then 'n/a'
         else trim(cntry)
    end as cntry
    from bronze.erp_loc_a101
    set @end_time=GETDATE();
	print'>>Load Duration:' + cast(datediff (second,@start_time ,@end_time) as nvarchar) + 'seconds';
	print'>>---------------------';
 
	set @start_time=GETDATE();
    print'>> Truncating Table: silver.erp_px_cat_g1v2'
    truncate table silver.erp_px_cat_g1v2
    print'>> Inserting Inro Data:silver.erp_px_cat_g1v2'
    insert into silver.erp_px_cat_g1v2(id,cat,subcat,maintainance)
    select
    id ,
    cat,
    subcat,
    maintainance
    from bronze.erp_px_cat_g1v2
    set @end_time=GETDATE();
	print'>>Load Duration:' + cast(datediff (second,@start_time ,@end_time) as nvarchar) + 'seconds';
	print'>>---------------------';

    set @batch_end_time=GETDATE();
	print'=================================================';
	print'Loading Silver Layer is completed';
	print'>>Total Load Duration:' + cast(datediff (second,@start_time ,@end_time) as nvarchar) + 'seconds';
	print'=================================================';

 end try 
begin catch
   print'==========================================';
   print'error occured during bronze layer';
   print'Error Message' + ERROR_MESSAGE();
   PRINT'Error Message' + cast(ERROR_NUMBER() AS NVARCHAR);
   PRINT'Error Message' + cast(ERROR_STATE()AS NVARCHAR);
   PRINT'===========================================';
   END CATCH
end 

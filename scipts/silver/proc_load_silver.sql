/*
===========================================================================================================================
Stored Procedure: Load Silver Layer (Bronze to Silver)
===========================================================================================================================
Script Purpose:
  This stored procedure performs the ETL (extract, transform, load) process to 
  populate the 'silver schema tables from the 'bronze' schema.
  Actions Performed:
  - Truncates Silver Tables.
  - Inserts transformed and cleansed data from Bronze into Silver Tables.

  Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

  Usage Example:  
    EXEC Silver.load_silver;
===========================================================================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver as 
BEGIN
	Declare @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		Print'=============================================';
		Print 'Loading Silver Layer';
		print'=============================================';

		Print'---------------------------------------------';
		PRINT'Loading CRM Tables';
		Print'---------------------------------------------';
		--Loading silver.crm_cust_info
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info
	print '>> Inserting Data Into: silver.crm_cust_info';
	Insert INTO silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_material_status,
		cst_gndr,
		cst_create_date
		)
    
	Select 
	cst_id,
	cst_key,
	trim(cst_firstname) as cst_firstname,
	trim(cst_lastname) as cst_lastname,
	CASE
    WHEN Upper(trim(cst_material_status)) ='S' THEN 'Single'
		 When Upper(trim(cst_material_status)) ='M' THEN 'Married'
		 Else 'n/a'
	end cst_material_status,

	CASE WHEN Upper(trim(cst_gndr)) ='F' THEN 'Female'
		 When Upper(trim(cst_gndr)) ='M' THEN 'Male'
		 Else 'n/a'
		end as cst_gndr,
	cst_create_date
	from (
	Select 
    *,
	ROW_NUMBER() OVER (PARTITION BY cst_id Order BY cst_create_date DESC) as flag_last
	from bronze.crm_cust_info
	where cst_id is not null
	)t 
	where flag_last =1
	SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + cast(DateDiff(Second, @start_time, @end_time) as nvarchar) + ' seconds';
		PRINT '--------------------------';

	--Loading silver.crm_prd_info
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info
	print '>> Inserting Data Into: silver.crm_prd_info';
	INSERT INTO silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
	)
	Select 
		prd_id,
		REPLACE(Substring(prd_key, 1, 5), '-', '_') as cat_id,
		Substring(prd_key, 7, len(prd_key)) as prd_key,
		prd_nm,
		ISNULL(prd_cost, 0) as prd_cost,
		CASE UPPER(TRIM(prd_line))
			 WHEN 'M' THEN 'Mountain'
			 WHEN 'R' THEN 'Road'
			 WHEN 'S' THEN 'Other Sales'
			 WHEN 'T' THEN 'Touring'
			 Else 'n/a'
		End as prd_line,
		Cast(prd_start_dt as DATE) as prd_start_dt,
		Cast(
    Lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1
    as date
    ) as prd_end_dt
	from bronze.crm_prd_info
	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + cast(DateDiff(Second, @start_time, @end_time) as nvarchar) + ' seconds';
	PRINT '--------------------------';

	--Loading crm_sales_details
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details
	print '>> Inserting Data Into: silver.crm_sales_details';
	INSERT INTO silver.crm_sales_details (
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
	)
	Select 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE WHEN sls_order_dt <= 0 or LEN (sls_order_dt) != 8 THEN NULL
		 Else cast(CAST(sls_order_dt as varchar) as date)
	end as sls_order_dt,
	CASE WHEN sls_ship_dt <= 0 or LEN (sls_ship_dt) != 8 THEN NULL
		 Else cast(CAST(sls_ship_dt as varchar) as date)
	end as sls_ship_dt,
	CASE WHEN sls_due_dt <= 0 or LEN (sls_due_dt) != 8 THEN NULL
		 Else cast(CAST(sls_due_dt as varchar) as date)
	end as sls_due_dt,
	CASE WHEN sls_sales IS NULL OR sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price)
			THEN sls_quantity * ABS(sls_price)
			Else sls_sales
	end as sls_sales,
	sls_quantity,
	CASE WHEN sls_price is null or sls_price <= 0
			THEN sls_sales/ Nullif(sls_quantity,0)
		Else sls_price
	End as sls_price
	from bronze.crm_sales_details
	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + cast(DateDiff(Second, @start_time, @end_time) as nvarchar) + ' seconds';
	PRINT '--------------------------';

	--Loading erp_cust_az12
	SET @end_time = GETDATE();
	PRINT '>> Truncating Table: silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12
	print '>> Inserting Data Into: silver.erp_cust_az12';
	INSERT INTO silver.erp_cust_az12 (
    cid, 
    bdare, 
    gen
    )
	select 
	case WHEN cid like 'NAS%' THEN SUBSTRING(cid, 4, len(cid))
		Else cid
	end cid,
	CASE WHEN bdare > GETDATE() THEN NULL	
		 Else bdare
	end as bdare,
	CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		 Else 'n/a'
	END AS gen
	from bronze.erp_cust_az12
	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + cast(DateDiff(Second, @start_time, @end_time) as nvarchar) + ' seconds';
	PRINT '--------------------------';

	--Loading erp-loc_a101
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101
	print '>> Inserting Data Into: silver.erp_loc_a101';
	INSERT INTO silver.erp_loc_a101(
	cid, 
  cntry
)
	Select 
	REPLACE(cid, '-', '') cid,
	CASE 
     WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		 WHEN TRIM(cntry) in ('US' , 'USA') THEN 'United States'
		 WHEN TRIM(cntry) = '' or cntry IS NULL THEN 'n/a'
		 ELSE TRIM(cntry)
	end as cntry
	from bronze.erp_loc_a101
	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + cast(DateDiff(Second, @start_time, @end_time) as nvarchar) + ' seconds';
	PRINT '--------------------------';

	--Loading erp_px_cat_g1v2
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2
	print '>> Inserting Data Into: silver.erp_px_cat_g1v2';
	INSERT INTO silver.erp_px_cat_g1v2(
	id,
  cat, 
  subcat, 
  maintenance
  )
	select 
	id,
	cat,
	subcat,
	maintenance
	from bronze.erp_px_cat_g1v2
	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + cast(DateDiff(Second, @start_time, @end_time) as nvarchar) + ' seconds';
	PRINT '--------------------------';

	SET @batch_end_time = GETDATE();
		PRINT '============================================'
		Print 'Loading Silver Layer is completed:';
		Print '	- Total Load Duration: ' + Cast(DateDiff(second, @batch_start_time, @batch_end_time) as nvarchar) + ' seconds';
		Print '============================================'

	END TRY
	BEGIN CATCH
		Print '=========================================='
		Print 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		Print 'Error Message' + ERROR_MESSAGE();
		Print 'Error Message' + CAst(ERROR_MESSAGE() as nvarchar);
		Print 'Error Message' + CAst(ERROR_STATE() as nvarchar);
		Print '=========================================='
	END CATCH
END


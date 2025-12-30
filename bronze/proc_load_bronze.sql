/*
========================================================================================
Stored Procedure: Load Bronze Layer (Source to Bronze)
========================================================================================
Scipt Purpose:
  This stored procedure loads data into the 'bronze' schema from external CSV files.
  It performs the following actions:
-Truncates the bronze tables before loading data.
-Uses the 'Bulk Insert' command to load data from csv files to bronze tables.

Parameters:
  None.
  This stored procedure does not accept any parameters or return any values.
Usage Example: 
  EXEC bronze.load_bronze;
========================================================================================
/*

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
Begin
	Declare @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		Print'=============================================';
		Print 'Loading Bronze Layer';
		print'=============================================';

		Print'---------------------------------------------';
		PRINT'Loading CRM Tables';
		Print'---------------------------------------------';

		SET @start_time = GETDATE();
		PRINT'>>Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT'>>Inserting Date Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\Steven\Documents\SQL Server Management Studio 22\sql\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + cast(DateDiff(Second, @start_time, @end_time) as nvarchar) + ' seconds';
		PRINT '--------------------------';

		SET @start_time = GETDATE();
		PRINT'>>Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT'>>Inserting Date Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\Steven\Documents\SQL Server Management Studio 22\sql\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + cast(DateDiff(Second, @start_time, @end_time) as nvarchar) + ' seconds';
		PRINT '--------------------------';

		SET @start_time = GETDATE();
		PRINT'>>Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT'>>Inserting Date Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\Steven\Documents\SQL Server Management Studio 22\sql\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + cast(DateDiff(Second, @start_time, @end_time) as nvarchar) + ' seconds';
		PRINT '--------------------------';

		Print'---------------------------------------------';
		PRINT'Loading ERP Tables';
		Print'---------------------------------------------';

		SET @start_time = GETDATE();
		PRINT'>>Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT'>>Inserting Date Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\Steven\Documents\SQL Server Management Studio 22\sql\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + cast(DateDiff(Second, @start_time, @end_time) as nvarchar) + ' seconds';
		PRINT '--------------------------';

		SET @start_time = GETDATE();
		PRINT'>>Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT'>>Inserting Date Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\Steven\Documents\SQL Server Management Studio 22\sql\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + cast(DateDiff(Second, @start_time, @end_time) as nvarchar) + ' seconds';
		PRINT '--------------------------';

		SET @start_time = GETDATE();
		PRINT'>>Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT'>>Inserting Date Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Steven\Documents\SQL Server Management Studio 22\sql\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + cast(DateDiff(Second, @start_time, @end_time) as nvarchar) + ' seconds';
		PRINT '--------------------------';

		SET @batch_end_time = GETDATE();
		PRINT '============================================'
		Print 'Loading Bronze Layer is completed:';
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

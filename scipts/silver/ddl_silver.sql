/*
================================================================================================
DDL Script: Create Silver Tables
================================================================================================
Script Purpose"
  This scipt creates tables in the 'silver' schema, dropping existing tables
  if they already exist.
Run this script to re-define te DDL structure of 'bronze' Tables
================================================================================================
*/

		IF OBJECT_ID('silver.crm_cust_info', 'U') is not null
			DROP TABLE silver.crm_cust_info;
GO
  
		Create TABLE silver.crm_cust_info (
			cst_id INT,
			cst_key nvarchar(50),
			cst_firstname nvarchar(50),
			cst_lastname nvarchar(50),
			cst_material_status nvarchar(50),
			cst_gndr nvarchar(50),
			cst_create_date date,
			dwh_create_date DATETIME2 DEFAULT GETDATE()
		);
Go

		IF OBJECT_ID('silver.crm_prd_info', 'U') is not null
			DROP TABLE silver.crm_prd_info;
GO
  
		Create TABLE silver.crm_prd_info (
			prd_id INT,
			cat_id nvarchar(50),
			prd_key nvarchar(50),
			prd_nm nvarchar(50),
			prd_cost INT,
			prd_line nvarchar(50),
			prd_start_dt DATE,
			prd_end_dt DATE,
			dwh_create_date DATETIME2 DEFAULT GETDATE()
		);
GO
  
		IF OBJECT_ID('silver.crm_sales_details', 'U') is not null
			DROP TABLE silver.crm_sales_details;
--GO
  
		Create TABLE silver.crm_sales_details (
			sls_ord_num nvarchar(50),
			sls_prd_key nvarchar(50),
			sls_cust_id INT,
			sls_order_dt DATE,
			sls_ship_dt DATE,
			sls_due_dt DATE,
			sls_sales INT,
			sls_quantity INT,
			sls_price INT,
			dwh_create_date DATETIME2 DEFAULT GETDATE()
		);
GO

		IF OBJECT_ID('silver.erp_loc_a101', 'U') is not null
			DROP TABLE silver.erp_loc_a101;
GO
  
		Create TABLE silver.erp_loc_a101 (
			cid nvarchar(50),
			cntry nvarchar(50),
			dwh_create_date DATETIME2 DEFAULT GETDATE()
		);
Go
  
		IF OBJECT_ID('silver.erp_cust_az12', 'U') is not null
			DROP TABLE silver.erp_cust_az12;
GO
  
		Create TABLE silver.erp_cust_az12 (
			cid nvarchar(50),
			bdare DATE,
			gen nvarchar(50),
			dwh_create_date DATETIME2 DEFAULT GETDATE()
		);
GO

		IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') is not null
			DROP TABLE silver.erp_px_cat_g1v2;
GO
  
		Create TABLE silver.erp_px_cat_g1v2 (
			id nvarchar(50),
			cat nvarchar(50),
			subcat nvarchar(50),
			maintenance nvarchar(50),
			dwh_create_date DATETIME2 DEFAULT GETDATE()
		);
GO

/*
===========================================================================================
Quality Checks
===========================================================================================
Script Purpose:
  This scipt performs various quality checks for data consistency, accuracy,
and standardication across the 'silver' schemas. It includes checks for:
- Null or duplicate primary keys.
- Unwanted Spaces in string fields.
- Data Standardization and consistency.
- Invalid data ranges and orders.
- Data consistently between related fields.

Usage Notes:
  - Run these checks after data loading silver layer.
  - Investigate and resolve any discrepancies found during the checks.
===========================================================================================
*/


-- ==============================================================================
--Checking 'silver.crm_info'
=================================================================================
--Check for nulls or duplicates in primary key
--Expectation: No Result
Select 
cst_id,
Count(*)
from silver.crm_cust_info
GROUP BY cst_id
Having Count(*) > 1 or cst_id is null;

--check for unwanted spaces
--expect: no results
Select cst_firstname
from silver.crm_cust_info
where cst_lastname!= trim(cst_lastname)

--data standardization & consistency
Select Distinct cst_gndr
from silver.crm_cust_info

select* from silver.crm_cust_info

-- ==============================================================================
-- Checking 'silver.crm_prd_info
-- ==============================================================================
--Check for nulls or duplicates in primary key
--Expectation: No Result
Select 
prd_id,
Count(*)
from silver.crm_prd_info
GROUP BY prd_id
Having Count(*) > 1 or prd_id is null;

--check for unwanted spaces
-- Expectation: No Results
Select 
  prd_nm
from silver.crm_prd_info
Where prd_nm != TRIM(prd_nm)

--checks for nulls or negative numbers
-- Expectation
Select 
  prd_cost
from silver.crm_prd_info
Where prd_cost<0 or prd_cost is null;

--data standarization & consistency
Select Distinct 
  prd_line
from silver.crm_prd_info;

--check for invalid date orders
--Expectation: No results
Select 
  * 
from silver.crm_prd_info
Where prd_end_dt < prd_start_dt;

-- ==============================================================================
-- Checking 'silver.crm_sales_details
-- ==============================================================================
--check for invalid dates
-- Expectation: No Invalid Dates
Select
NULLIF(sls_due_dt,0) sls_due_dt
from bronze.crm_sales_details
where sls_due_dt <= 0 
or LEN (sls_due_dt) != 8
or sls_due_dt > 20500101
or sls_due_dt < 19000101

--check for invalid date orders
-- Expectation: No Results
Select 
  *
from silver.crm_sales_details
where sls_order_dt > sls_ship_dt 
  or  sls_order_dt > sls_due_dt 

--check data consistency: between sales, quantity, and price
-- >> Sales = Quantity * pricec
Select DISTINCT
sls_sales,
sls_quantity,
sls_price
from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null
or sls_quantity is null
or sls_price is null
or sls_sales <= 0
or sls_quantity <= 0 
or sls_price <=0
ORDER BY sls_sales, sls_quantity, sls_price

-- ==============================================================================
-- Checking 'silver.erp_cust_az12'
-- ==============================================================================
-- Identify OUT-of-Range Dates
-- Expecation: Birthdates between 1924-01-01 and Today
Select Distinct
bdare
from silver.erp_cust_az12
where bdare < '1924-01-01' 
  or bdare > GetDate();

--data standardization & consistency
Select Distinct 
gen
from silver.erp_cust_az12

-- ==============================================================================
-- Checking 'silver.erp_loc_a101'
-- ==============================================================================
-- Data Standardization & Consistency
Select Distinct
  cntry
from silver.erp_loc_a101
Order By cntry;

-- ==============================================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- ==============================================================================
--check for unwanted spaces
-- expectation: no results
Select 
  *
  from silver.erp_px_cat_g1v2
where cat != trim(cat)
  or subcat != trim(subcat)
  or maintenance != trim(maintenance)

--data standardization & consistency
Select Distinct 
maintenance
from silver.erp_px_cat_g1v2;

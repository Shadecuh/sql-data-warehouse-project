/*
=======================================================================================
DDL Scipt Purpose:
=======================================================================================
  Script Purpose:
    This scipt creates views for the Gold Layer in the data warehouse.
    The Gold Layer represents the final dimension and the fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer
    to produce a clean, enriched, and business-ready dataset.

Usage
    - These views can be queried directly for analytics and reporting
=======================================================================================
*/

-- =======================================================================================
-- Create Dimension: gold.dim_customers
-- =======================================================================================
IF Object_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO
  
CREATE VIEW gold.dim_customers AS 
select 
	ROW_NUMBER() OVER (ORDER BY cst_id) as customer_key,
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	la.cntry as country,
	ci.cst_material_status as marital_status,
	CASE 
  WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM IS THE MASTER FOR GENDER INFO
		Else COALESCE(ca.gen, 'n/a')
	END AS gender,
	ca.bdare as birthdate,
	ci.cst_create_date as create_date
from silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
on		  ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
on		  ci.cst_key = la.cid
GO

-- =======================================================================================
-- Create Dimension: gold_dim_products
-- =======================================================================================
IF Object_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products as 
Select 
ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt,pn.prd_key) as product_key,
pn.prd_id as product_id,
pn.prd_key as product_number,
pn.prd_nm as product_name,
pn.cat_id as category_id,
pc.cat as category,
pc.subcat subcategory,
pc.maintenance,
pn.prd_cost as cost,
pn.prd_line as product_line,
pn.prd_start_dt as start_date
from silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
on pn.cat_id =  pc.id
WHERE pn.prd_end_dt is null;  --filter out all historical data
GO

-- =======================================================================================
-- Create Fact Table: gold.fact_sales
-- =======================================================================================
IF Object_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales as 
select 
sd.sls_ord_num as order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt as order_date,
sd.sls_ship_dt as shipping_date,
sd.sls_due_dt as due_date,
sd.sls_sales as sales_amount,
sd.sls_quantity as quantity,
sd.sls_price as price
from silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
on sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
on sd.sls_cust_id = cu.customer_id
Go
  
/*
Select * 
from gold.fact_sales f 
LEFT JOIN gold.dim_customers c
on c.customer_key =  f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key =f.product_key
where c.customer_key is null
*/




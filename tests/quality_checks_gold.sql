
/*
=================================================================================
Quality Checks
=================================================================================
Script Purpose:
    This scipt performs quality checks to validate the integrity, consistency,
    and accuracy of the cold layer. these checks ensure:
    -Uniqueness of surrogate keys in dimension tables.
    -Referential integirty between fact and dimension tables.
    -Validation of relationships in the data model for analytical purposes.

Usage Notes:
    -Run these checks after data loading silver layer.
    - Investigate and resolve any discrepancies found during the checks
=================================================================================
*/

-- =================================================================================
-- Checking 'gold.dim_customers'
-- =================================================================================
--Check for Uniqueness of customer key in gold.dim_customers
--Expectation: No results
Select
  customer_key,
  COUNT(*) AS duplicate_count
from gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- =================================================================================
- Checking 'gold.product_key'
-- =================================================================================
--Check for Uniqueness of Product Key in gold.dim_products
--Expectation: No results
Select
  product_key,
  COUNT(*) AS duplicate_count
from gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;
  
-- =================================================================================
-- Checking 'gold.fact_sales'
-- =================================================================================
--Checks the data model connectivity between fact and dimensions
Select * 
from gold.fact_sales f 
LEFT JOIN gold.dim_customers c
on c.customer_key =  f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key =f.product_key
where c.customer_key is null

/*
============================================================

Customer Report

============================================================

Purpose:
    - This report consolidates key customer metrics and behaviors

Highlights:
    1. Gathers essential fields such as names, ages, and transaction details.
    2. Segments customers into categories (VIP, Regular, New) and age groups.
    3. Aggregates customer-level metrics:
        - total orders
        - total sales
        - total quantity purchased
        - total products
        - lifespan (in months)
    4. Calculates valuable KPIs:
        - recency (months since last order)
        - average order value
        - average monthly spend

============================================================
*/
CREATE VIEW gold.report_customers AS
WITH base_query as (
/* 1) Base Query: Retrieves core columns from tables */
Select
f.order_number,
f.product_key,
f.order_date,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
c.first_name,
c.last_name,
CONCAT(c.first_name, '', c.last_name) as customer_name,
DATEDIFF (year,c.birthdate, GETDATE()) age
from gold.fact_sales f
LEFT JOIN gold.dim_customers c 
on c.customer_key = f.customer_key
where order_date is not null)

, customer_aggregation as (
/* 2) Customer Aggregations: Summarize key metrics at the customer level */
Select 
customer_key,
customer_number,
customer_name,
age,
COUNT(DISTINCT order_number) as total_orders,
SUM(sales_amount) as total_sales,
sum(quantity) as total_quantity,
count(distinct product_key) as total_products,
MAX(order_date) as last_order_date,
DATEDIFF (month, MIN(order_date), MAX(order_date)) as lifespan
from base_query
GROUP BY 
    customer_key,
    customer_number,
    customer_name,
    age
)

Select 
customer_key,
customer_number,
customer_name,
age,
CASE WHEN age <20 THEN 'Under 20'
     When age between 20 and 29 THEN '20-29'
     When age between 30 and 39 then '30-39'
     When age between 40 and 49 then '40-49'
     Else '50 and above'
end as age_group,
CASE WHEN lifespan >= 12 and total_sales >5000 then 'VIP'
	 When lifespan >= 12 and total_sales <=5000 then 'Regular'
	 Else 'New'
end customer_segment,
last_order_date,
DATEDIFF (month, last_order_date, GETDATE())as recency,
total_orders,
total_sales,
total_quantity,
total_products,
lifespan,
--Compute averafe order value (avo)
CASE WHEN total_orders = 0 then 0
     Else total_sales / total_orders
end as avg_order_value,

--compute average monthly spent
CASE WHEN lifespan = 0 THEN total_sales
     Else total_sales / lifespan
end as avg_monthly_spend
from customer_aggregation

/*Select 
age_group,
count(customer_number) as total_customers,
SUM(total_sales) total_sales
from gold.report_customers
GROUP BY age_group

Select 
customer_segment,
count(customer_number) as total_customers,
SUM(total_sales) total_sales
from gold.report_customers
GROUP BY customer_segment */

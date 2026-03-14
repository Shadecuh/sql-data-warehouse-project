/*
============================================================
Product Report
============================================================

Purpose:
    - This report consolidates key product metrics and behaviors.

Highlights:
    1. Gathers essential fields such as product name, category, subcategory, and cost.
    2. Segments products by revenue to identify High-Performers, Mid-Range, or Low-Performers.
    3. Aggregates product-level metrics:
        - total orders
        - total sales
        - total quantity sold
        - total customers (unique)
        - lifespan (in months)
    4. Calculates valuable KPIs:
        - recency (months since last sale)
        - average order revenue (AOR)
        - average monthly revenue

============================================================
*/
Create View gold.report_products AS
with base_query as (
/* 1) Base Query: Retrieves core columns from tables */
Select
f.order_number,
f.order_date,
f.customer_key,
f.sales_amount,
f.quantity,
p.product_key,
p.product_name,
p.category,
p.subcategory,
p.cost
from gold.fact_sales f
LEFT JOIN gold.dim_products p 
on f.product_key = p.product_key
where order_date is not null
),

product_aggregation as (
/* 2) Product Aggregations: Summarize key metrics at the customer level */
Select 
Product_key,
Product_name,
category,
subcategory,
cost,
DATEDIFF (month, MIN(order_date), MAX(order_date)) as lifespan,
MAX(order_date) as last_sale_date,
COUNT(DISTINCT order_number) as total_orders,
COUNT(DISTINCT customer_key) as total_customers,
SUM(sales_amount) as total_sales,
sum(quantity) as total_quantity,
round(avg(cast(sales_amount as float) / NULLIF(quantity, 0)),1) as avg_selling_price
from base_query
GROUP BY 
    Product_key,
    Product_name,
    category,
    subcategory,
    cost
)

/* 2) Final Query: Combines all product results into one output */

Select 
    Product_key,
    Product_name,
    category,
    subcategory,
    cost,
    last_sale_date,
    DATEDIFF(month,last_sale_date, GETDATE()) as recency_in_months,
CASE 
     WHEN total_sales > 50000 THEN 'High-Performer'
     WHEN total_sales >= 10000 THEN 'Mid-Range'
     Else 'Low-Performer'
end as product_segment,
lifespan,
total_orders,
total_sales,
total_quantity,
total_customers,
avg_selling_price,
-- Average order Revenue (AOR)
CASE 
     WHEN total_sales = 0 then 0
	 Else total_sales / total_orders
end avg_order_revenue,

--Average monthly revenue
CASE 
     WHEN lifespan = 0 then total_sales
     Else total_sales / lifespan
end as avg_monthly_revenue

from product_aggregation

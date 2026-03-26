/* 
======================================================================================
Customer Report
======================================================================================
Purpose:
   -This report consolidates key customer metrics and behaviors

Highlights:
 1.Gathers essential fields such as names, ages and transactional details.
 2.Segments Customer into categories (VIP,Regular,New) and age groups.
 3.Aggregated customer-level metrics:
   - total orders
   - total sales
   - total quantity purchased
   - total products
   - lifespan (in months)
 4. Calculate valuable KPI:
  -recency (months since last order)
  -average order value
  -average monthly spend

========================================================================================
*/

create view gold_report_customers as
with base_query as(
/*--------------------------------------------------------------------------------------
1) Base Query: Retrieves core columns from tables
----------------------------------------------------------------------------------------*/
select 
f.order_number,
f.product_key,
f.order_date,
f.sales,
f.quantity,
c.country,
c.marital_status,
c.gender,
c.customer_key,
c.customer_number,
CONCAT(c.fist_name,' ',c.last_name) as customer_name,
DATEDIFF(year,c.birthdate,GETDATE())as Age
from gold.fact_sales f
left join gold.dim_customers c
on f.customer_key=c.customer_key
where order_date  is not  null)

,customer_aggregrtion as(
/*--------------------------------------------------------------------
2)Customer Aggregation: Summarizes key metrics at the customer level
----------------------------------------------------------------------*/
select
customer_key,
customer_number,
customer_name,
country,
marital_status,
gender,
Age,
COUNT(distinct order_number) as total_orders,
sum(sales) as total_sales,
SUM(quantity) as total_quantity,
COUNT(distinct product_key) as total_products,
MAX(order_date) as last_order_date,
DATEDIFF(month,MIN(order_date),MAX(order_date)) as lifespan
from base_query
group by
	customer_key,
	customer_number,
	customer_name,
	Age,
	country,
    marital_status,
    gender

)
select
customer_key,
customer_number,
customer_name,
country,
marital_status,
gender,
case when Age < 20 then 'Under 20'
     when Age between 20 and 29 then '20-29'
	 when Age between 30 and 39 then '30-39'
	 when Age between 40 and 49 then '40-49'
	 else '50 and above'
end as age_group,
case when lifespan>=12 and total_sales>50000 then 'VIP'
     when lifespan>=12 and total_sales<=50000 then 'Regular'
	 else 'New'
end as Customer_Segment,
DATEDIFF(month,last_order_date,getdate()) as recency,
total_orders,
total_sales,
total_quantity,
total_products,
last_order_date,
lifespan,
--- Compute Average order value
case when total_orders =0 then 0
     else total_orders/total_orders 
end as avg_order_value,
---Compute average monthly spend
case when lifespan =0 then total_sales 
     else total_sales/lifespan
end as	avg_monthly_spend
from customer_aggregrtion


select * from gold_report_customers

select 
country,
marital_status,
COUNT(customer_number) as total_customer,
SUM(total_sales) as total_sales
from gold_report_customers
group by country,
marital_status

select 
country,
SUM(total_sales) as total_sales
from gold_report_customers
group by country
order by total_sales desc
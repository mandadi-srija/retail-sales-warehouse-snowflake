use warehouse compute_wh;
use database snowflake_learning_db;
create or replace table GOLD.MONTHLY_SALES AS
select 
    date_trunc('month',transaction_date) as sales_month,
    sum(sales_price) as total_sales
from silver.fact_sales
group by sales_month;

select * from silver.fact_sales;
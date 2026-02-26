use warehouse compute_wh;
use database snowflake_learning_db;
create schema if not exists GOLD;
create or replace table gold.sales_by_state as
select 
   d.state,
   sum(f.sales_price) as total_sales
from silver.fact_sales f
join silver.customer_dim d
on f.CUSTOMER_SK=d.CUSTOMER_SK
group by d.state;

select * from gold.sales_by_state ;
select * from silver.customer_dim;
select * from silver.fact_sales;
create or replace table gold.sales_by_year as
select
  year(transaction_date) as sales_year,
  sum(sales_price) as total_sales
from silver.fact_sales
group by sales_year;

select * from gold.sales_by_year limit 5;
--sales by store every month
create or replace table GOLD.MONTHLY_SALES as 
select 
  date_trunc('month',transaction_date) as sales_month,
  sum(sales_price) as total_sales
from silver.fact_sales
group by sales_month;
--top 10
create or replace table GOLD.TOP_STORES as 
select 
   store_id,
   sum(sales_price) as total_sales
  from silver.fact_sales
  group by store_id
  order by total_sales desc limit 10;

  select count(*) from gold.sales_by_state;
  select count(*) from gold.sales_by_year;
  select count(*) from gold.monthly_sales;
  select count(*) from gold.top_stores;


  alter table silver.fact_sales
  cluster by (transaction_date);
  select sum(sales_price) from silver.fact_sales;
  select sum(total_sales) from gold.sales_by_state;
  select sum(sales_price) from gold.sales_by_year;
  select sum(sales_price) from gold.monthly_sales;
  select sum(sales_price) from gold.top_stores;
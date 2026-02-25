desc table snowflake_sample_data.tpcds_sf100tcl.customer;

desc table snowflake_sample_data.tpcds_sf100tcl.customer_address;

use warehouse compute_wh;

use database snowflake_learning_db;

create or replace table silver.customer_dim(
    customer_sk NUMBER AUTOINCREMENT,
    customer_id NUMBER,
    state STRING,
    start_date DATE,
    end_date DATE,
    is_current STRING
);
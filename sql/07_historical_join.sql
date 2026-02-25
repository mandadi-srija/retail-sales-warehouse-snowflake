USE WAREHOUSE COMPUTE_WH;
USE DATABASE SNOWFLAKE_LEARNING_DB;

CREATE OR REPLACE TABLE SILVER.STORE_SALES_ENRICHED AS
SELECT
    ss.ss_customer_sk,
    ss.ss_store_sk,
    ss.ss_sales_price,
    d.d_date AS transaction_date
FROM SILVER.STORE_SALES_CLEAN ss
JOIN SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.DATE_DIM d
    ON ss.ss_sold_date_sk = d.d_date_sk;

-- ACtual historical join

SELECT
    f.transaction_date,
    d.state,
    SUM(f.ss_sales_price) AS total_sales
FROM SILVER.STORE_SALES_ENRICHED f
JOIN SILVER.CUSTOMER_DIM d
    ON f.ss_customer_sk = d.customer_id
    AND f.transaction_date >= d.start_date
    AND (f.transaction_date < d.end_date OR d.end_date IS NULL)
GROUP BY
    f.transaction_date,
    d.state
ORDER BY
    f.transaction_date;
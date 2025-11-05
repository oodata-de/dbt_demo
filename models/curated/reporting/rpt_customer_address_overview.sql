{{ 
  config(
    materialized='view'
    ) 
}}
-- REPORTING: Customer with addresses and order metrics

WITH fact AS (
  SELECT
    CUSTOMER_SK,
    DATE_KEY,
    QUANTITY,
    AMOUNT
  FROM {{ ref('f_sales_order_line') }}
),
addr AS (
  SELECT
    CUSTOMER_SK,
    ADDRESS_SK
  FROM {{ ref('br_customer_address') }}
),
d_c AS (
  SELECT
    CUSTOMER_SK,
    CUSTOMER_ID,
    MASKED_EMAIL,
    COUNTRY,
    FIRST_ORDER_DATE
  FROM {{ ref('d_customer') }}
),
metrics AS (
  SELECT
    CUSTOMER_SK,
    COUNT(DISTINCT DATE_KEY) AS ACTIVE_DAYS,
    SUM(QUANTITY) AS TOTAL_QTY,
    SUM(AMOUNT) AS TOTAL_AMOUNT
  FROM fact
  GROUP BY CUSTOMER_SK
)

SELECT
  d_c.CUSTOMER_SK,
  d_c.CUSTOMER_ID,
  d_c.MASKED_EMAIL,
  d_c.COUNTRY,
  d_c.FIRST_ORDER_DATE,
  m.ACTIVE_DAYS,
  m.TOTAL_QTY,
  m.TOTAL_AMOUNT,
  COUNT(DISTINCT a.ADDRESS_SK) AS ADDRESS_COUNT
FROM d_c
LEFT JOIN metrics m ON d_c.CUSTOMER_SK = m.CUSTOMER_SK
LEFT JOIN addr a ON d_c.CUSTOMER_SK = a.CUSTOMER_SK
GROUP BY
  d_c.CUSTOMER_SK,
  d_c.CUSTOMER_ID,
  d_c.MASKED_EMAIL,
  d_c.COUNTRY,
  d_c.FIRST_ORDER_DATE,
  m.ACTIVE_DAYS,
  m.TOTAL_QTY,
  m.TOTAL_AMOUNT
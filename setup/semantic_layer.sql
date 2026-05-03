create warehouse transforming; 
create database raw;
create database analytics; 
create schema raw.jaffle_shop;

create or replace table raw.jaffle_shop.customers 
( id varchar,
  name varchar );

 copy into raw.jaffle_shop.customers (id, name)
 from 's3://dbt-learn-sample-data/raw_customers.csv'
 file_format = (  
   type = 'CSV' 
   field_delimiter = ',' 
   skip_header = 1 );

 create or replace table raw.jaffle_shop.orders
 ( id varchar,
   customer varchar,
   ordered_at timestamp,
   store_id varchar,
   subtotal int,
   tax_paid int,
   order_total int
 );

 copy into raw.jaffle_shop.orders 
 (id,customer,ordered_at,store_id,subtotal,tax_paid,order_total)
 from 's3://dbt-learn-sample-data/raw_orders.csv'
 file_format = (
   type = 'CSV'
   field_delimiter = ','
   skip_header = 1
 );

 create or replace table raw.jaffle_shop.items
 ( id varchar,
   order_id varchar,
   sku varchar
 );

 copy into raw.jaffle_shop.items (id,order_id,sku)
 from 's3://dbt-learn-sample-data/raw_items.csv'
 file_format = (
   type = 'CSV'
   field_delimiter = ','
   skip_header = 1
 );

 create or replace table raw.jaffle_shop.products 
 ( sku varchar,
   name varchar,
   type varchar,
   price int,
   description varchar
 );

 copy into raw.jaffle_shop.products (sku,name,type,price,description)
 from 's3://dbt-learn-sample-data/raw_products.csv'
 file_format = (
   type = 'CSV'
   field_delimiter = ','
   skip_header = 1
 );

 create or replace table raw.jaffle_shop.stores 
 ( id varchar,
   name varchar,
   opened_at datetime,
   tax_rate int
  );

 copy into raw.jaffle_shop.stores(id,name,opened_at,tax_rate)
 from 's3://dbt-learn-sample-data/raw_stores.csv'
 file_format = (
   type = 'CSV'
   field_delimiter = ','
   skip_header = 1
 );

 create or replace table raw.jaffle_shop.supplies
 ( id varchar,
   name varchar,
   cost int,
   perishable boolean,
   sku varchar
  );

 
 copy into raw.jaffle_shop.supplies (id,name,cost,perishable,sku)
 from 's3://dbt-learn-sample-data/raw_supplies.csv'
 file_format = (
   type = 'CSV'
   field_delimiter = ','
   skip_header = 1
   );


select * from raw.jaffle_shop.customers;
select * from raw.jaffle_shop.orders;
select * from raw.jaffle_shop.items;
select * from raw.jaffle_shop.products;
select * from raw.jaffle_shop.stores;
select * from raw.jaffle_shop.supplies;
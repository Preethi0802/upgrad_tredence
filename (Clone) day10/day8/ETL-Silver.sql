-- Databricks notebook source
-- MAGIC %run /Workspace/Users/preethi_1724384753078@npupgradassessment.onmicrosoft.com/day10/day8/includes

-- COMMAND ----------



-- COMMAND ----------

create table if not exists gold.category_count as select category, count(*) as count from upgrad_databricks.silver.products_silver group by category order by count desc

-- COMMAND ----------

select * from gold.category_count

-- COMMAND ----------

create table if not exists silver.products_silver as (select ProductID as product_id, ProductName as product_name ,Category as category, ListPrice as list_price from day10.gold)

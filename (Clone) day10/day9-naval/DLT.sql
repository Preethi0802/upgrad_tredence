-- Databricks notebook source
create streaming live table customers 
COMMENT "This customers table"
TBLPROPERTIES ("myCompanyPipeline.quality" = "bronze")
as 
select * from cloud_files("dbfs:/mnt/upgradtrendenceadls/delta/files/dlt_files/customers/", "csv",map("cloudFiles.inferColumnTypes","True"))

-- COMMAND ----------

create streaming live table sales 
COMMENT "This sales table"
TBLPROPERTIES ("myCompanyPipeline.quality" = "bronze")
as 
select * from cloud_files("dbfs:/mnt/upgradtrendenceadls/delta/files/dlt_files/sales/", "csv",map("cloudFiles.inferColumnTypes","True"))

-- COMMAND ----------

create streaming live table sales_silver
(
  constraint order_id expect (order_id is not null) on violation drop row
)
as
select distinct(*) from stream(LIVE.sales)

-- COMMAND ----------

create streaming live table sales_customer
COMMENT "This sales_customer table"
TBLPROPERTIES ("myCompanyPipeline.quality" = "silver")
as 
(select s.order_id,s.customer_id, c.customer_name,c.customer_city, c.customer_state, s.quantity,s.total_amount 
from stream(live.sales_silver) s
inner join live.customers c
on s.customer_id=c.customer_id)

-- COMMAND ----------

create LIVE table customer_state_count as 
select customer_state, count(customer_state) as count from LIVE.sales_customer group by customer_state

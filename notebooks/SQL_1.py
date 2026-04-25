# Databricks notebook source
# MAGIC %sql
# MAGIC select * from workspace.default.customers_100 as w

# COMMAND ----------

# MAGIC %sql
# MAGIC select Country, Company, r from ( select *, row_number() over(partition by Country order by Company) as r from workspace.default.customers_100) r where r == 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from (select *, dense_rank() over(partition by Country order by City) as rn from workspace.default.customers_100) where rn == 1

# COMMAND ----------

# MAGIC %sql
# MAGIC ---find duplicates
# MAGIC
# MAGIC select Country, count(*) from workspace.default.customers_100 group by Country having count(*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC ---delect duplicates
# MAGIC DELETE FROM workspace.default.customers_100 WHERE Country IN (SELECT Country FROM (SELECT Country, Company, ROW_NUMBER() OVER(PARTITION BY Country ORDER BY Company) AS r FROM workspace.default.customers_100) r WHERE r > 1)

# COMMAND ----------

# DBTITLE 1,Cell 3
# MAGIC %sql
# MAGIC select Company,Country,rn from ( select Company, Country, row_number() over(partition by Country order by Company) as rn from workspace.default.customers_100 ) t where rn > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from workspace.default.customers_100 where Country in (select Country from ( select Company, Country, row_number() over(partition by Country order by Company) as rn from workspace.default.customers_100) t where rn > 1)

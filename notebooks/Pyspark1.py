# Databricks notebook source
# DBTITLE 1,create dataframe
data = [
    (1, "Anil",   "IT",      60000, "2024-01-01"),
    (2, "Ravi",   "HR",      45000, "2024-01-03"),
    (3, "Kiran",  "IT",      60000, "2024-01-05"),
    (4, "Suresh", "Finance", None,  "2024-01-07"),
    (5, "Anil",   "IT",      70000, "2024-01-10"),
    (6, "Ravi",   "HR",      45000, "2024-01-12"),
    (7, "Meena",  "Finance", 80000, "2024-01-15")
]

cols = ["emp_id", "name", "dept", "salary", "join_date"]
df=spark.createDataFrame(data, cols)
df.show()

# COMMAND ----------

# DBTITLE 1,filter
df1=df.filter(df.dept=="IT")
df1.show()

# COMMAND ----------

# DBTITLE 1,fillna with 0
df2=df.fillna({"salary":0})
df2.show()

# COMMAND ----------

# DBTITLE 1,remove duplicates based on two col's
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number, desc

WindowSpec=Window.partitionBy("dept", "name").orderBy(desc("join_date"))

df3=df.withColumn("rn", row_number().over(WindowSpec)).filter(col("rn") == 1)
df3.show()

# COMMAND ----------

# DBTITLE 1,dept wise emp count
from pyspark.sql.functions import count
df4=df.groupBy("dept").count()
df4.show()

# COMMAND ----------

# DBTITLE 1,max salary
from pyspark.sql import functions as F
df5=df.agg(F.max("salary")).show()

# COMMAND ----------

# DBTITLE 1,max, min
from pyspark.sql import functions as F
df6=df2.groupBy("dept").agg(F.max("salary").alias("max_salary"), F.min("salary").alias("min_salary")).show()

# COMMAND ----------

# DBTITLE 1,avg, mean, median
from pyspark.sql import functions as F
df7=df2.groupBy("dept").agg(F.avg("salary").alias("avg_salary"), F.mean("salary").alias("mean_salary"), F.median("salary").alias("median_salary")).show()

# COMMAND ----------

# DBTITLE 1,Sort employees by salary (descending)
from pyspark.sql.functions import col, row_number, desc
df8=df.orderBy(desc("salary")).show()

# COMMAND ----------

# DBTITLE 1,Add bonus column (10% of salary)
df9=df2.withColumn("Bonus", col("salary")*0.1).show()

# COMMAND ----------

# DBTITLE 1,Convert join_date string to date
from pyspark.sql.functions import col, to_date
df10=df2.withColumn("join_date", to_date(col("join_date"), "yyyy-MM-dd"))

# COMMAND ----------

# DBTITLE 1,Employees joined after 2024-01-05 -- str type
from pyspark.sql.functions import col, filter
df11=df2.filter(col("join_date") > "2024-01-05").show()

# COMMAND ----------

# DBTITLE 1,Employees joined after 2024-01-05 -- date type
from pyspark.sql.functions import col, to_date, lit

df12 = df10.filter(col("join_date") > "2024-01-05")
df12.show()

# COMMAND ----------

from pyspark.sql.functions import col, dense_rank
from pyspark.sql.window import Window
w = Window.orderBy(col("salary").desc())
df13=df2.withColumn("rn",dense_rank().over(w)).filter(col("rn") == 2)
df13.show()


# COMMAND ----------

# DBTITLE 1,Add row number per department (by salary)
from pyspark.sql.functions import col, dense_rank,row_number
from pyspark.sql.window import Window
w = Window.partitionBy("dept").orderBy(col("salary").desc())
df14=df2.withColumn("rank",row_number().over(w))
df14.show()

# COMMAND ----------

# DBTITLE 1,Highest salary per department
from pyspark.sql.functions import max
df15=df2.groupBy("dept").agg(max("salary")).show()

# COMMAND ----------

# DBTITLE 1,Find highest paid employee in each department
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window
w = Window.partitionBy("dept").orderBy(col("salary").desc())
df16=df2.withColumn("rank",row_number().over(w)).filter(col("rank") == 1)
display(df16)

# COMMAND ----------

df17=df2.withColumnRenamed("salary", "salary_new").show()

# COMMAND ----------

df18=df2.filter(df2.name.startswith("R")).show()

# COMMAND ----------

df19=df2.groupBy("salary").count().filter("count>1").show()

# COMMAND ----------

from pyspark.sql.functions import *
data1 = [
    ("M1", 1000, "-"),
    ("E1", 400, "M1"),
    ("E2", 300, "M1"),
    ("M2", 2000, "-"),
    ("E3", 100, "M2"),
    ("E4", 200, "M2")
]

cols = ["empid", "salary", "manager_id"]

df20 = spark.createDataFrame(data1, cols)
df21=df20.withColumn("mng_id",when(col("manager_id")=="-",col("empid")).otherwise(col("manager_id")))\
    .withColumn("is_emp",when(col("manager_id")=="-",lit(0)).otherwise(lit(1)))
df21.show()

# COMMAND ----------

# DBTITLE 1,Cell 22
df21_alias = df21.alias("a")
df20_alias = df20.alias("b")
df_new = df21_alias.join(df20_alias, on=(df21_alias.mng_id == df20_alias.empid), how="inner") \
    .select(
        df21_alias.empid,
        df21_alias.salary,
        df21_alias.is_emp,
        df20_alias.empid.alias("manager_id"),
        df20_alias.salary.alias("manager_salary")
    )
df_new.show()

# COMMAND ----------

# DBTITLE 1,Fix AttributeError in cell 23
from pyspark.sql.functions import desc

df_new.orderBy(desc("manager_salary"), "is_emp", desc("salary")).select("empid").show()

# COMMAND ----------

from pyspark.sql.functions import col,lit
df22=df.filter(col("join_date") > lit("2024-01-05")).show()

# COMMAND ----------

from pyspark.sql.functions import year, month,col,dayofmonth
df23=df.select(
    "emp_id",
    year(col("join_date")).alias("year"),
    month(col("join_date")).alias("month"),
    dayofmonth(col("join_date")).alias("day")).show()



# COMMAND ----------

from pyspark.sql.functions import year, month,col
df24=df.filter(month("join_date")==1).show()

# COMMAND ----------

from pyspark.sql.functions import year, month,col,lit,datediff
df25=df.withColumn("count_days", datediff(col("join_date"), lit("2024-01-01"))).show()

# COMMAND ----------

from pyspark.sql.functions import year, month,col,lit,datediff,current_date
df25=df.withColumn("count_days", datediff(current_date() ,col("join_date"))).show()

# COMMAND ----------

# DBTITLE 1,Cell 29
from pyspark.sql.functions import year, month, col, lit, datediff, current_date, max, desc
df25 = df.withColumn("count_days", datediff(current_date(), col("join_date")))\
    .orderBy(desc("count_days"))\
    .agg(max("count_days"))\
    .show()

# COMMAND ----------

df26=df.orderBy(col("join_date").asc()).limit(1).show()

# COMMAND ----------

df26=df.orderBy(col("join_date").desc()).limit(1).show()

# COMMAND ----------

from pyspark.sql.functions import year, month, col, lit, datediff, current_date, max, desc, lag
from pyspark.sql.window import Window
w=Window.orderBy(col("join_date"))
df27=df.withColumn("prv_date", lag("join_date", 1).over(w))\
    .withColumn("diff", datediff(col("join_date"), col("prv_date")))\
    .show()


# COMMAND ----------

from pyspark.sql.functions import year, month, col, lit, datediff,date_sub,current_date
df28=df.filter(col("join_date")>= date_sub(current_date(), 776)).show()

# COMMAND ----------

from pyspark.sql.functions import year, month, col, lit, datediff,date_sub,current_date,date_add
df29=df.withColumn("probation_end_date", date_add(col("join_date"), 10)).show()

# COMMAND ----------

from pyspark.sql.functions import year, month, col, lit, datediff,date_sub,current_date,date_add,count,weekofyear
df30=df.groupBy(weekofyear(col("join_date")).alias("week")).count().orderBy("week").show()

# COMMAND ----------

from pyspark.sql.functions import year, month, col, lit
df31=df.groupBy(month(col("join_date")).alias("month")).count().show()

# COMMAND ----------

from pyspark.sql.functions import year, month, col, lit, dayofweek
df32=df.filter(dayofweek(col("join_date")).isin(1,7)).show()

# COMMAND ----------

# DBTITLE 1,Cell 38
from pyspark.sql.functions import year, month, col, lit, dayofweek
df32=df.filter(~dayofweek(col("join_date")).isin(1,7)).show()

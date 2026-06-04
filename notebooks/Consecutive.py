# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ConsecutiveExample").getOrCreate()

data = [
    (1,),
    (2,),
    (3,),
    (5,),
    (6,),
    (8,)
]

df = spark.createDataFrame(data, ["id"])

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("consecutive_test_table")

# COMMAND ----------

# DBTITLE 1,Find Consecutive ID Groups
# MAGIC %sql
# MAGIC select min(id) as min_id, max(id) as max_id from (
# MAGIC     select id, id - row_number() over(order by id) as grp from consecutive_test_table
# MAGIC ) t group by grp;

# COMMAND ----------

from pyspark.sql.functions import row_number, col, min, max
from pyspark.sql.window import Window

w=Window.orderBy(col("id"))
df1=df.withColumn("grp", col("id") - row_number().over(w))
df2=df1.groupBy("grp").agg(min("id").alias("min_id"), max("id").alias("max_id"))
display(df2)

# COMMAND ----------

# DBTITLE 1,Find Missing Numbers
# MAGIC %sql
# MAGIC select id+1 as missing_id from
# MAGIC (select id, lead(id) over(order by id) as next_id from consecutive_test_table)
# MAGIC t where next_id - id > 1

# COMMAND ----------

from pyspark.sql.functions import lead,col
from pyspark.sql.window import Window

w=Window.orderBy(col("id"))
df1=df.withColumn("next_id", lead("id").over(w))
df2=df1.filter(col("next_id")-col("id")>1).select(col("id")+1).alias("missing_id")
display(df2)

# COMMAND ----------

from pyspark.sql import SparkSession

# Create Spark Session
spark = SparkSession.builder.appName("LogsExample").getOrCreate()

# Sample data
data = [
    (1, 'A'),
    (2, 'A'),
    (3, 'A'),
    (4, 'B'),
    (5, 'B'),
    (6, 'C'),
    (7, 'C'),
    (8, 'C')
]

# Create DataFrame
df_new = spark.createDataFrame(data, ["id", "status"])

# Show DataFrame
df_new.show()

# COMMAND ----------

df_new.write.mode("overwrite").saveAsTable("consecutive_test_table2")

# COMMAND ----------

# DBTITLE 1,Find 3 Consecutive Same Values
# MAGIC %sql
# MAGIC select distinct status from (
# MAGIC     select status, lag(status, 1) over(order by id) as p1,
# MAGIC     lag(status, 2) over(order by id) as p2 from consecutive_test_table2)
# MAGIC t where p1 = status and p2 = status

# COMMAND ----------

from pyspark.sql.functions import lag,col
from pyspark.sql.window import Window

w=Window.orderBy(col("id"))
df1=df_new.withColumn("p1", lag("status", 1).over(w))\
    .withColumn("p2", lag("status", 2).over(w))
df2=df1.filter((col("status")==col("p1")) & (col("p1")==col("p2")))
df3=df2.select(col('id'),col('status'))
display(df3)

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ConsecutiveLogin").getOrCreate()

data = [
    (101, '2026-05-01'),
    (101, '2026-05-02'),
    (101, '2026-05-03'),
    (101, '2026-05-06'),

    (102, '2026-05-01'),
    (102, '2026-05-03'),
    (102, '2026-05-04'),
    (102, '2026-05-05'),

    (103, '2026-05-02'),
    (103, '2026-05-03'),

    (104, '2026-05-01'),
    (104, '2026-05-02'),
    (104, '2026-05-03'),
    (104, '2026-05-04')
]

df_new1 = spark.createDataFrame(data, ["emp_id", "login_date"])

df_new1.write.mode("overwrite").saveAsTable("consecutive_test_table3")

# COMMAND ----------

# DBTITLE 1,Consecutive Login Days
# MAGIC %sql
# MAGIC select emp_id from (
# MAGIC SELECT emp_id,
# MAGIC        login_date,
# MAGIC        DATE_SUB(
# MAGIC          login_date,
# MAGIC          ROW_NUMBER() OVER(
# MAGIC              PARTITION BY emp_id
# MAGIC              ORDER BY login_date
# MAGIC          )
# MAGIC        ) grp
# MAGIC FROM consecutive_test_table3 ) t
# MAGIC group by emp_id,grp having count(*) > 3

# COMMAND ----------

from pyspark.sql.functions import row_number,date_sub,count,col
from pyspark.sql.window import Window

w = Window.partitionBy("emp_id").orderBy("login_date")
          
df1= df_new1.withColumn("grp", date_sub(col("login_date"), row_number().over(w)))
df2=df1.groupBy("emp_id","grp").agg(count("login_date").alias("count"))
df3=df2.filter(col("count")>=3)
display(df3)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from (
# MAGIC     select id, lag(id) over(order by id) as p1 from consecutive_test_table
# MAGIC ) t where id=p1

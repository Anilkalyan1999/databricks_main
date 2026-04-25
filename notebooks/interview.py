# Databricks notebook source
# MAGIC %md
# MAGIC ### ## # Create Dataframe

# COMMAND ----------

dup_data = [
        (1, "Anil", "2026-02-01 10:00:00"),
        (1, "Anil", "2026-02-02 11:00:00"),
        (2, "Ravi", "2026-02-01 09:30:00"),
        (3, "Kiran", "2026-02-01 08:00:00"),
        (3, "Kiran", "2026-02-03 12:00:00")
    ]

df_org = spark.createDataFrame(dup_data, ['id','name','time_stp'])

# COMMAND ----------

df_org1 = spark.createDataFrame([
        (1, "Anil", "2026-02-01 10:00:00"),
        (1, "Anil", "2026-02-02 11:00:00"),
        (2, "Ravi", "2026-02-01 09:30:00"),
        (3, "Kiran", "2026-02-01 08:00:00"),
        (3, "Kiran", "2026-02-03 12:00:00")
    ], ['id','name','time_stp'])
df_org1.show()
df_org1.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### # Removing duplicate records from the dataset

# COMMAND ----------

df=df_org.dropDuplicates(['id'])
display(df)

# COMMAND ----------

df_org.createOrReplaceTempView('dup')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from (select *, row_number() over(partition by id order by time_stp desc) as rn from dup) t where rn=1

# COMMAND ----------

from pyspark.sql.functions import max,min
df=df_org.agg(
    max('time_stp').alias('max_time'),
    min('time_stp').alias('min_time')
)
display(df)

# COMMAND ----------

# DBTITLE 1,Fix NameError and typos in window function usage
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

df = df_org.withColumn("rn", row_number().over(Window.partitionBy('id').orderBy('id'))).filter("rn = 1")
display(df)

# COMMAND ----------

# DBTITLE 1,Fix AttributeError in window function usage
from pyspark.sql.window import Window
import pyspark.sql.functions as f

w = Window.partitionBy('id').orderBy(f.desc('time_stp'))
df1 = df_org.withColumn('rn', f.row_number().over(w)).filter("rn = 1").drop('rn')
display(df1)

# COMMAND ----------

# DBTITLE 1,Cell 11
from pyspark.sql.functions import upper, col, lit, filter,replace

df_org2 = df_org1.select('name','id')
#df_org2.show()
df_org3 = df_org2.withColumn("name_new", upper(col("name")))
display(df_org3)

# COMMAND ----------

df_org4=df_org3.withColumn("country", lit("India"))
df_org4=df_org4.withColumnRenamed("name_new","NAMES")
df_org4=df_org4.drop("name")
display(df_org4)

# COMMAND ----------

# DBTITLE 1,Fix TypeError in DataFrame.filter usage
df_org5 = df_org4.filter(col('id') != 0)
display(df_org5)

# COMMAND ----------

from pyspark.sql.functions import when
df_org6=df_org5.withColumn("id", when (col("id")==1 , 8) .otherwise(col("id")))
df_org6.show()

# COMMAND ----------

df_org5 = df_org4.filter(col("id").isNotNull())  # transformation

# COMMAND ----------

# DBTITLE 1,Fix AttributeError in DataFrame orderBy usage
from pyspark.sql.functions import desc
df_org7 = df_org5.orderBy(desc('id')).show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, rank

w=Window.orderBy(desc('id'))
df_org8=df_org5.withColumn("rn", dense_rank().over(w)).filter("rn = 1").drop('rn')
display(df_org8)

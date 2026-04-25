# Databricks notebook source
df = spark.read.option('multiline','true').json('/Volumes/workspace/default/json/sample_file_json2.json')

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import col

df1=df.select(
    col("id"),
    col("name"),
    col("address.city").alias("city"),
    col("address.pincode").alias("pincode"),
    col("skills").alias("skills")
)
display(df1)

# COMMAND ----------

from pyspark.sql.functions import explode

df2=df1.select("id","name","city","pincode",explode("skills").alias("skill"))
display(df2)

# COMMAND ----------

df_n = spark.read.option('multiline','true').json('/Volumes/workspace/default/json/sample_file_json3.json')
display(df_n)

# COMMAND ----------

df_n1=df_n.select(
    col("order_id"),
    col("customer.name"),
    explode("customer.contacts").alias("contacts")
).select("order_id","name","contacts.type","contacts.value")
display(df_n1)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

df = spark.read \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .option("multiLine", "false") \
    .schema(schema) \
    .json("/Volumes/workspace/default/json/sample_file_json4.json")

df.show(truncate=False)

# COMMAND ----------

df_n1.write.mode("overwrite").json("/Volumes/workspace/default/json/sample_file_json_output.json")

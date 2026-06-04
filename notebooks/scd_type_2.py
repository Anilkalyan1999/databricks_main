# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# ----------------------------
# Source DataFrame
# ----------------------------
source_data = [
    (1, "Anil",  "IT",      65000),
    (2, "Ravi",  "HR",      45000),
    (3, "Kiran", "IT",      70000),
    (4, "Meena", "Finance", 80000)
]

source_schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("dept", StringType(), True),
    StructField("salary", IntegerType(), True)
])

source_df = spark.createDataFrame(source_data, source_schema)

# ----------------------------
# Target DataFrame
# ----------------------------
target_data = [
    (1, "Anil",  "IT", 60000, "2024-01-01", None, "Y"),
    (2, "Ravi",  "HR", 45000, "2024-01-01", None, "Y"),
    (3, "Kiran", "IT", 70000, "2024-01-01", None, "Y")
]

target_schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("dept", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("start_date", StringType(), True),
    StructField("end_date", StringType(), True),
    StructField("is_current", StringType(), True)
])

target_df = spark.createDataFrame(target_data, target_schema)

# ----------------------------
# Display Output
# ----------------------------
display(source_df)
display(target_df)

# COMMAND ----------

target_df.write.format("delta").mode("overwrite").saveAsTable("target_table")

# COMMAND ----------

source_df.write.format("delta").mode("overwrite").saveAsTable("source_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY target_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from source_table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from target_table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO target_table t
# MAGIC USING source_table s
# MAGIC ON t.emp_id = s.emp_id
# MAGIC AND t.is_current = 'Y'
# MAGIC
# MAGIC WHEN MATCHED
# MAGIC AND (
# MAGIC        t.name   <> s.name
# MAGIC     OR t.dept   <> s.dept
# MAGIC     OR t.salary <> s.salary
# MAGIC )
# MAGIC THEN UPDATE SET
# MAGIC     t.end_date   = CURRENT_DATE(),
# MAGIC     t.is_current = 'N';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO target_table
# MAGIC
# MAGIC SELECT
# MAGIC     s.emp_id,
# MAGIC     s.name,
# MAGIC     s.dept,
# MAGIC     s.salary,
# MAGIC     CURRENT_DATE() AS start_date,
# MAGIC     NULL AS end_date,
# MAGIC     'Y' AS is_current
# MAGIC
# MAGIC FROM source_table s
# MAGIC
# MAGIC LEFT JOIN target_table t
# MAGIC ON s.emp_id = t.emp_id
# MAGIC AND t.is_current = 'Y'
# MAGIC
# MAGIC WHERE t.emp_id IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from target_table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from target_table

# COMMAND ----------

# MAGIC %md
# MAGIC **SCD Type 2 in pyspark**

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# ----------------------------
# Source DataFrame
# ----------------------------
source_data = [
    (1, "Anil",  "IT",      65000),
    (2, "Ravi",  "HR",      45000),
    (3, "Kiran", "IT",      70000),
    (4, "Meena", "Finance", 80000)
]

source_schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("dept", StringType(), True),
    StructField("salary", IntegerType(), True)
])

source_df = spark.createDataFrame(source_data, source_schema)

# ----------------------------
# Target DataFrame
# ----------------------------
target_data = [
    (1, "Anil",  "IT", 60000, "2024-01-01", None, "Y"),
    (2, "Ravi",  "HR", 45000, "2024-01-01", None, "Y"),
    (3, "Kiran", "IT", 70000, "2024-01-01", None, "Y")
]

target_schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("dept", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("start_date", StringType(), True),
    StructField("end_date", StringType(), True),
    StructField("is_current", StringType(), True)
])

target_df = spark.createDataFrame(target_data, target_schema)

# ----------------------------
# Display Output
# ----------------------------
display(source_df)
display(target_df)

# COMMAND ----------

target_df.write.format("delta").mode("overwrite").saveAsTable("target_table")

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_date, lit

# Delta table reference
delta_table = DeltaTable.forName(spark, "target_table")

# Step 1: Expire old records
delta_table.alias("t").merge(
    source_df.alias("s"),
    "t.emp_id = s.emp_id AND t.is_current = 'Y'"
).whenMatchedUpdate(
    condition="""
           t.name   <> s.name
        OR t.dept   <> s.dept
        OR t.salary <> s.salary
    """,
    set={
        "end_date": "current_date()",
        "is_current": "'N'"
    }
).execute()

# Step 2: Insert new + changed records
current_target = spark.table("target_table").filter("is_current = 'Y'")
display(current_target)

new_records = source_df.alias("s").join(
    current_target.alias("t"),
    on="emp_id",
    how="left_anti"
)
display(new_records)

final_df = new_records.withColumn("start_date", current_date()) \
    .withColumn("end_date", lit(None)) \
    .withColumn("is_current", lit("Y"))

display(final_df)

# Append records
final_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("target_table")

# View final output
# display(spark.table("target_table"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from target_table

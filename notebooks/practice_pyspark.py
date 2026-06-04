# Databricks notebook source
notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().rsplit("/", 1)[1]
print(notebook_name)

# COMMAND ----------

from pyspark.sql import SparkSession
from datetime import datetime

data = [
    (100, "Amit", "Maths", "10", "A", "Central School", 2024, 
     101, "Male", 15, "Hyderabad", "Telangana", "India", 92.5, 450, "A",
     "Mr. Sharma", "Final", "2024-03-15", "Pass", "Excellent",
     "SCH001", "MATH101", "2024-03-15 10:20:30"),

    (50, "Amit", "Physics", "10", "A", "Central School", 2024,
     101, "Male", 15, "Hyderabad", "Telangana", "India", 92.5, 450, "A",
     "Mrs. Gupta", "Mid Term", "2024-02-10", "Pass", "Needs Improvement",
     "SCH001", "PHY101", "2024-02-10 09:10:15"),

    (20, "Amit", "History", "10", "A", "Central School", 2024,
     101, "Male", 15, "Hyderabad", "Telangana", "India", 92.5, 450, "A",
     "Mr. Khan", "Unit Test", "2024-01-20", "Fail", "Low Score",
     "SCH001", "HIS101", "2024-01-20 11:00:05"),

    (75, "Amit", "Chemistry", "10", "A", "Central School", 2024,
     101, "Male", 15, "Hyderabad", "Telangana", "India", 92.5, 450, "A",
     "Dr. Rao", "Final", "2024-03-16", "Pass", "Good",
     "SCH001", "CHEM101", "2024-03-16 10:45:12"),

    (80, "Akshay", "History", "11", "B", "National Public School", 2024,
     102, "Male", 16, "Mumbai", "Maharashtra", "India", 88.0, 430, "B",
     "Mr. Khan", "Final", "2024-03-15", "Pass", "Good Improvement",
     "SCH002", "HIS101", "2024-03-15 11:30:40"),

    (15, "Akshay", "Maths", "11", "B", "National Public School", 2024,
     102, "Male", 16, "Mumbai", "Maharashtra", "India", 88.0, 430, "B",
     "Mr. Sharma", "Unit Test", "2024-01-22", "Fail", "Very Low Score",
     "SCH002", "MATH101", "2024-01-22 09:55:20"),

    (30, "Akshay", "Physics", "11", "B", "National Public School", 2024,
     102, "Male", 16, "Mumbai", "Maharashtra", "India", 88.0, 430, "B",
     "Mrs. Gupta", "Mid Term", "2024-02-14", "Fail", "Needs Practice",
     "SCH002", "PHY101", "2024-02-14 10:15:55"),

    (90, "Anil", "Maths", "12", "C", "Sri Chaitanya", 2023,
     103, "Male", 17, "Bengaluru", "Karnataka", "India", 95.0, 470, "A+",
     "Mr. Sharma", "Final", "2023-03-12", "Pass", "Excellent",
     "SCH003", "MATH101", "2023-03-12 10:05:18"),

    (72, "Anil", "Physics", "12", "C", "Sri Chaitanya", 2023,
     103, "Male", 17, "Bengaluru", "Karnataka", "India", 95.0, 470, "A+",
     "Mrs. Gupta", "Mid Term", "2023-02-05", "Pass", "Good",
     "SCH003", "PHY101", "2023-02-05 09:40:33"),

    (95, "Anil", "Chemistry", "12", "C", "Sri Chaitanya", 2023,
     103, "Male", 17, "Bengaluru", "Karnataka", "India", 95.0, 470, "A+",
     "Dr. Rao", "Final", "2023-03-18", "Pass", "Outstanding",
     "SCH003", "CHEM101", "2023-03-18 11:25:00"),

    (82, "Sneha", "Maths", "11", "A", "Delhi Public School", 2023,
     104, "Female", 16, "Delhi", "Delhi", "India", 90.5, 455, "A",
     "Mr. Sharma", "Final", "2023-03-14", "Pass", "Very Good",
     "SCH004", "MATH101", "2023-03-14 10:10:22"),

    (91, "Sneha", "Physics", "11", "A", "Delhi Public School", 2023,
     104, "Female", 16, "Delhi", "Delhi", "India", 90.5, 455, "A",
     "Mrs. Gupta", "Final", "2023-03-14", "Pass", "Excellent",
     "SCH004", "PHY101", "2023-03-14 10:30:10"),

    (76, "Sneha", "History", "11", "A", "Delhi Public School", 2023,
     104, "Female", 16, "Delhi", "Delhi", "India", 90.5, 455, "A",
     "Mr. Khan", "Mid Term", "2023-02-12", "Pass", "Good",
     "SCH004", "HIS101", "2023-02-12 09:55:12"),

    (60, "Pooja", "Maths", "12", "B", "Little Flower School", 2024,
     105, "Female", 17, "Chennai", "Tamil Nadu", "India", 85.0, 420, "B",
     "Mr. Sharma", "Mid Term", "2024-02-18", "Pass", "Average",
     "SCH005", "MATH101", "2024-02-18 11:11:11"),

    (69, "Pooja", "Biology", "12", "B", "Little Flower School", 2024,
     105, "Female", 17, "Chennai", "Tamil Nadu", "India", 85.0, 420, "B",
     "Dr. Rao", "Final", "2024-03-20", "Pass", "Good",
     "SCH005", "BIO101", "2024-03-20 08:45:33")
]

columns = [
    "marks", "Student_name", "Subjects", "Class", "Section", "School", "Year",
    "Roll_No", "Gender", "Age", "City", "State", "Country",
    "Attendance", "Total_Marks", "Grade",
    "Teacher_Name", "Exam_Type", "Exam_Date", "Pass_Fail", "Remarks",
    "School_ID", "Subject_Code", "Record_Created_Timestamp"
]

df = spark.createDataFrame(data, columns)

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql import functions as f
agg_df=df.groupBy("Student_name").agg(
    f.min("marks").alias("min_marks"),
    f.max("marks").alias("max_marks")
)
display(agg_df)

# COMMAND ----------

# DBTITLE 1,adding new column
from pyspark.sql.functions import collect_list, count, when, col, collect_set
df_agg1=df.groupBy("Student_name", "Pass_Fail")\
    .agg(collect_list("Subjects").alias("Subjects"), count("Subjects").alias("Total_passed"))\
        .withColumn("promoted", when((col("Total_passed")>=2) & (col("Pass_Fail")=="Pass"), "Yes").otherwise("No"))
display(df_agg1)
    

# COMMAND ----------

from pyspark.sql.functions import posexplode

df23 = df_agg1.select("Student_name", "Subjects")

df_agg23 = df23.select(
    "Student_name",
    posexplode("Subjects").alias("position", "Subject")
)

display(df_agg23)

# COMMAND ----------

from pyspark.sql.functions import explode
from pyspark.sql import functions as f
df_agg2=df_agg1.select("Student_name","Subjects")
df_agg2=df_agg2.select("Student_name", explode("Subjects").alias("Sub"))

display(df_agg2)

# COMMAND ----------

from pyspark.sql.functions import collect_list, count, when, col, split
# Sample data
data_ex = [
    ("Demo", "1_name"),
    ("Demo", "3_age"),
    ("Demo", "2_gender"),
    ("Vitals", "1_bp"),
    ("Vitals", "2_sugar")
]

columns = ["Dataset_name", "Column_name"]
df_ex = spark.createDataFrame(data_ex, columns)
processed_df = df_ex.withColumn("Column_position", split(col("Column_name"), "_")[0].cast("int"))\
    .withColumn("column_name", split(col("Column_name"), "_")[1])\
        .select("column_position", "column_name", "Dataset_name")\
        .orderBy("Column_position")
display(processed_df)


'''In sql
SELECT 
CAST(split(Column_name, '_')[0] AS INT) AS column_position,
split(Column_name, '_')[1] AS column_name,
Dataset_name
FROM df_ex
ORDER BY column_position'''

# COMMAND ----------

# DBTITLE 1,lag function
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, when, sum as _sum, to_date
from pyspark.sql.window import Window

# Sample data
data_lag = [
    (11114, "2025-01-01 08:30:00", "I"),
    (11114, "2025-01-01 10:30:00", "O"),
    (11114, "2025-01-01 11:30:00", "I"),
    (11114, "2025-01-01 15:30:00", "O"),
    (11115, "2025-01-01 09:30:00", "I"),
    (11115, "2025-01-01 17:30:00", "O"),
]
#spark= sparksecssion.bulder.appname(emp_table).getorCreate()
df_lag = spark.createDataFrame(data_lag, ["emp_id","punch_time","flag"])
df_lag = df_lag.withColumn("punch_time", (col("punch_time").cast("timestamp")))\
    .withColumn("punch_date", to_date("punch_time"))
display(df_lag)
w=Window.partitionBy("emp_id","punch_date").orderBy("punch_time")

df_lag2=df_lag.withColumn("prev_punch_time", lag("punch_time", 1).over(w))\
    .withColumn("duration", when((col("flag") == "O") & (col("prev_punch_time").isNotNull()), (col("punch_time").cast("long") - col("prev_punch_time").cast("long"))/3600).otherwise(0))
display(df_lag2)
result = df_lag2.groupBy("emp_id", "punch_date").agg(_sum("duration").alias("duration"))
display(result)

# COMMAND ----------

# DBTITLE 1,remove duplicates and keep only the latest record
data = [
    (1, "Anil", "2025-12-01 10:00:00"),
    (1, "Anil", "2025-12-02 09:00:00"),  # latest
    (2, "Kalyan", "2025-12-01 08:00:00"),
    (2, "Kalyan", "2025-12-03 11:00:00") # latest
]

columns = ["id", "name", "update_ts"]

df = spark.createDataFrame(data, columns)
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number

window_spec = Window.partitionBy("id").orderBy(col("update_ts").desc())

latest_df = (
    df.withColumn("rn", row_number().over(window_spec))
      .filter(col("rn") == 1)
      .drop("rn")
)

latest_df.show()


# COMMAND ----------

# DBTITLE 1,ways to handle NULL values in PySpark
#Replace NULL values (fillna)
df.fillna(0)                    # Replace all numeric NULLs with 0
df.fillna("NA")                 # Replace all string NULLs with 'NA'
df.fillna({"age": 0, "name": "Unknown"})
#Remove rows with NULL values (dropna)
df.dropna()                     # Drop rows if any column is NULL
df.dropna(subset=["name"])      # Drop rows if 'name' is NULL
#Handle NULL using when / otherwise
from pyspark.sql.functions import when, col
df.withColumn(
    "salary",
    when(col("salary").isNull(), 0).otherwise(col("salary"))
)
#Check for NULL values
df.filter(col("salary").isNull())
df.filter(col("salary").isNotNull())

# COMMAND ----------

# DBTITLE 1,Find customers who have NOT made any purchases in the last 30 days.
customers = [
    (1, "Anil"),
    (2, "Kalyan"),
    (3, "Ravi")
]
cust_df = spark.createDataFrame(customers, ["cust_id", "name"])
orders = [
    (1, "2025-11-10"),
    (2, "2026-02-26")  # recent purchase
]
orders_df = spark.createDataFrame(orders, ["cust_id", "order_date"]) \
                 .withColumn("order_date", col("order_date").cast("date"))
from pyspark.sql.functions import current_date, col, date_sub

recent_orders = orders_df.filter(col("order_date") >= date_sub(current_date(),30))

result= cust_df.join(recent_orders, cust_df.cust_id == recent_orders.cust_id, "left_anti")
display(result)

# COMMAND ----------

result.explain()

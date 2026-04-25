# Databricks notebook source
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

data2 = [
    (101, "Amit", "MATH101", "SCH001", "2024-03-25", 1, "Excellent Performance"),
    (101, "Amit", "PHY101", "SCH001", "2024-03-26", 2, "Improved"),
    (101, "Amit", "HIS101", "SCH001", "2024-03-27", 3, "Poor Attendance"),

    (102, "Akshay", "MATH101", "SCH002", "2024-03-22", 4, "Needs Focus"),
    (102, "Akshay", "PHY101", "SCH002", "2024-03-21", 5, "Getting Better"),
    (102, "Akshay", "HIS101", "SCH002", "2024-03-20", 6, "Very Low Score"),

    (103, "Anil", "MATH101", "SCH003", "2023-03-18", 7, "Outstanding"),
    (103, "Anil", "PHY101", "SCH003", "2023-03-17", 8, "Good Improvement"),
    (103, "Anil", "CHEM101", "SCH003", "2023-03-16", 9, "Top Performer"),

    (104, "Sneha", "MATH101", "SCH004", "2023-03-15", 10, "Very Good"),
    (104, "Sneha", "PHY101", "SCH004", "2023-03-14", 11, "Excellent"),
    (104, "Sneha", "HIS101", "SCH004", "2023-03-13", 12, "Consistent"),

    (105, "Pooja", "MATH101", "SCH005", "2024-03-19", 13, "Average Student"),
    (105, "Pooja", "BIO101", "SCH005", "2024-03-18", 14, "Good"),
    
    (None, "Unknown", None, None, "2024-03-10", 99, "No Records Found")
]

columns2 = [
    "Roll_No", "Student_Name", "Subject_Code", "School_ID",
    "Record_Update_Date", "Update_ID", "Additional_Remarks"
]

df2 = spark.createDataFrame(data2, columns2)

# COMMAND ----------

df2.write.format("delta").mode("overwrite").saveAsTable("student_progress")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from student_progress

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from student_marks

# COMMAND ----------

# DBTITLE 1,sub_query- 3rd highest value
# MAGIC %sql
# MAGIC select max(Total_Marks) from student_marks where Total_Marks < (select max(Total_Marks) from student_marks where Total_Marks < (select max(Total_Marks) from student_marks where Total_Marks < (select max(Total_Marks) from student_marks))) 

# COMMAND ----------

# DBTITLE 1,highest value
# MAGIC %sql
# MAGIC select distinct Total_Marks, Student_name from student_marks order by Total_Marks desc limit 1 offset 3

# COMMAND ----------

# MAGIC %sql
# MAGIC select Total_Marks, rank from (select distinct Total_Marks, DENSE_RANK() over(order by Total_Marks desc) as rank from student_marks) a where rank=3

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from student_marks sm join student_progress sp on sm.Roll_No = sp.Roll_No

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   sm.Roll_No, 
# MAGIC   sm.Student_Name, 
# MAGIC   sp.Additional_Remarks 
# MAGIC from student_marks sm 
# MAGIC left join student_progress sp 
# MAGIC   on sm.Roll_No = sp.Roll_No 
# MAGIC where sp.Additional_Remarks is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   sm.Roll_No, 
# MAGIC   sm.Student_Name, 
# MAGIC   sp.Additional_Remarks 
# MAGIC from student_marks sm 
# MAGIC right join student_progress sp 
# MAGIC   on sm.Roll_No = sp.Roll_No 
# MAGIC where sp.Additional_Remarks is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   sm.Roll_No, 
# MAGIC   sm.Student_Name
# MAGIC from student_marks sm 
# MAGIC left anti join student_progress sp 
# MAGIC   on sm.Roll_No = sp.Roll_No

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   Student_name, 
# MAGIC   min(marks) as min_marks, 
# MAGIC   max(marks) as max_marks 
# MAGIC from student_marks
# MAGIC group by Student_name

# COMMAND ----------

# MAGIC %sql
# MAGIC select Student_name,Subjects, rank() over(partition by Student_name order by Subjects desc) as rank from student_marks

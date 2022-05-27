from pyspark.sql import SparkSession
spark=SparkSession.builder.master("local").appName("test").getOrCreate()
sc=spark.sparkContext
df=spark.read.csv("file:///H:\SampleData\\100Employees_record.csv",header=True)

# Joining three Columns i.e First Name, Middle Initials, Last Name as Name
from pyspark.sql.functions import col, concat, lit
df1=df.withColumn("Name",concat(col("First Name"),lit(' '),col("Middle Initial"),lit(' '),col("Last Name"))).drop("First Name","Middle Initial","Last Name")

# Removing Unwanted Columns
df1=df1.drop("Age in Yrs.","Weight in Kgs.","Quarter of Joining")


# Renaming Columns
df1=df1.withColumnRenamed("Date of Joining","DOJ").withColumnRenamed("Date of Birth","DOB").withColumnRenamed("Age in Company (Years)","Experience(samecompany)") \
            .withColumnRenamed("Last % Hike","Hike").withColumnRenamed("Phone No. ","Mobile")


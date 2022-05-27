from pyspark.sql import SparkSession
spark=SparkSession.builder.master("local").appName("test").getOrCreate()
sc=spark.sparkContext
df=spark.read.csv("file:///H:\SampleData\\100Employees_record.csv",header=True)


## crating merge conflict
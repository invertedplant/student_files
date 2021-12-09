import sys
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as f
# you may add more import if you need to

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 2").getOrCreate()
# YOUR CODE GOES BELOW
df = spark.read.option("header", True).csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))

# Drop rows where Price Range is None
df = df.na.drop(subset=["Price Range"])


# Partition by Price Range
w = Window.partitionBy("City", "Price Range")

result = df.withColumn("maxRating", f.max("Rating").over(w))\
        .withColumn("minRating", f.min("Rating").over(w))\
        .where((f.col("Rating") == f.col("maxRating")) | (f.col("Rating") == f.col("minRating")))\
        .drop("maxRating", "minRating")

result.write.option("header", True).csv("hdfs://%s:9000/assignment2/output/question2/" % (hdfs_nn))

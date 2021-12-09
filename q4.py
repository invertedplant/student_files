import sys
import ast
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 4").getOrCreate()
# YOUR CODE GOES BELOW
sc = spark.sparkContext

df = spark.read.option("header", True).csv("hdfs://%s:9000/assignment2/part1/input/" % hdfs_nn)
# df1 = df.select("City", explode("Cuisine Style")).groupby("Cuisine Style").count()
df1 = df.select("City", explode(split(col("Cuisine Style"), ",")).alias("Cuisine Style"))\
    .groupby("City", "Cuisine Style").count()
df1.show()

df1.write.option("header", True).csv("hdfs://%s:9000/assignment2/output/question4" % hdfs_nn)

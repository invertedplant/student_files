import ast
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# you may add more import if you need to


# don't change this line

hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()
# YOUR CODE GOES BELOW
sc = spark.SparkContext


def foreach(record):
    record_l = ast.literal_eval(record[1])
    idx = record[0]
    reviews = record_l[0]
    dates = record_l[1]
    out = []
    for i in range(len(reviews)):
        out.append((idx, reviews[i], dates[i]))

    return out


df = spark.read.option("header", True).csv("hdfs://%s:9000/assignment2/part1/input/" % hdfs_nn)
r = df.select("ID_TA", "Reviews")
rdd = r.rdd.map(lambda ls: [ls[0], ls[1]])
output = rdd.map(foreach)
out_data = list(output.collect())

d = []
for x in out_data:
    for y in x:
        d.append(y)

df_new = spark.createDataFrame(d, ["ID_TA", "review", "date"]).show()

df_new.write.option("header", True).csv("hdfs://%s:9000/assignment2/output/question3" % hdfs_nn)
# out_df.saveAsTextFile("hdfs://%s:9000/assignment2/output/question1" % (hdfs_nn))


spark.stop()

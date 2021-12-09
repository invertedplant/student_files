import ast
import sys
from pyspark.sql import SparkSession
from pyspark.sql import funcions as F
from operator import add
# you may add more import if you need to

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 5").getOrCreate()
# YOUR CODE GOES BELOW

df = spark.read.option("header", True).parquet("hdfs://%s:9000/assignment2/part2/input/" % (hdfs_nn))
# df.printSchema()
df = df.select("movie_id", "title", "cast") # from the hint

def func(x):
    movie_id = x[0]
    title = x[1]
    cast = ast.literal_eval(record[2])
    name_pairs = []

    for i in cast:
        name = i["name"] # get the names
        name_pairs.append(str(name))

    result = []

    # check the name pairs
    for i in range(len(name_pairs)):
        for j in range(i+1, len(name_pairs)):
            if name_pairs[i] > name_pairs[j]:
                result.append((movie_id, title, name_pairs[i], name_pairs[j]))
            else:
                result.append((movie_id, title, name_pairs[j], name_pairs[i]))
    return result

rdd = df.rdd.map(func).flatMap(lambda x: x)
df2 = spark.createDataFrame(rdd, ["movie_id", "title", "actor1", "actor2"]) # first 4 cols
count = df2.groupBy("actor1", "actor2").count().filter(F.col("count") >= 2)
df3 = df2.join(count, ['actor1', 'actor2']).drop(F.col("count"))
df3 = df3.iloc[:, :4] # first 4 cols
df3.write.mode('overwrite').parquet("hdfs://%s:9000/assignment2/output/question5" % (hdfs_nn))
spark.stop()
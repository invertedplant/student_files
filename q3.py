import ast
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# you may add more import if you need to


# don't change this line

hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()
# YOUR CODE GOES BELOW
sc = spark.sparkContext


def for_each(record):
    record_to_list = ast.literal_eval(record[1])
    ta_id = record[0]
    reviews = record_to_list[0]
    dates = record_to_list[1]
    out = []
    for i in range(len(reviews)):
        out.append((ta_id, reviews[i], dates[i]))

    return out


data_list = []

df = spark.read.option("header", True).csv("hdfs://%s:9000/assignment2/part1/input/" % hdfs_nn)
df_select = df.select("ID_TA", "Reviews")
df_rdd = df_select.df_rdd.map(lambda ls: [ls[0], ls[1]])
output = df_rdd.map(for_each)
output_data = list(output.collect())

for outputs in output_data:
    for review in outputs:
        data_list.append(review)

df_new = spark.createDataFrame(data_list, ["ID_TA", "review", "date"]).cache()
df_new.show()

df_new.write.option("header", True).csv("hdfs://%s:9000/assignment2/output/question3" % hdfs_nn)

# spark.stop()
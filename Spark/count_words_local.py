import findspark
findspark.init("/opt/spark-2.4.5/")
from operator import add
import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


# for SparkConf() check out http://spark.apache.org/docs/latest/configuration.html
conf = (SparkConf()
        .setMaster("local")
        .setAppName("WordCounter")
        .set("spark.executor.memory", "1g"))

spark = SparkSession.builder.appName('MyFirstProcessingApp').master('local').getOrCreate()


spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "<your access key>")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "<your secret key>>")
spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")

sc = spark.sparkContext

if __name__ == "__main__":
    inputFile = "s3a://sparkdemo-spring/input.txt"
    print("Counting words in ", inputFile)
    lines = sc.textFile(inputFile)

    # for lambdas check out https://docs.python.org/3/tutorial/controlflow.html#lambda-expressions
    lines_nonempty = lines.filter(lambda x: len(x) > 0)
    counts = lines_nonempty.flatMap(lambda x: x.split(' ')) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(add)
    output = counts.collect()
    for (word, count) in output:
        print("%s: %i" % (word, count))

    sc.stop()
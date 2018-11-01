from pyspark import SparkContext, SparkConf
from ast import literal_eval
import sys

inputFiles = sys.argv[1]
outputFiles = sys.argv[2]

conf = SparkConf().setAppName("Similarity Matrix")
sc = SparkContext(conf=conf)

similarity_matrix = sc.textFile(inputFiles)\
.map(lambda x: eval(x))\
.sortBy(lambda x: x[1], False)\


rdd = sc.parallelize(similarity_matrix.take(10), 1)
rdd.saveAsTextFile(outputFiles)

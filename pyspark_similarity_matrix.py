from pyspark import SparkContext, SparkConf
from ast import literal_eval
import sys

inputFiles = sys.argv[1]
outputFiles = sys.argv[2]
#inputFiles = '/bigd43/inverted_index/*'
#outputFiles = '/bigd43/similarity_matrix'

conf = SparkConf().setAppName("Similarity Matrix")
sc = SparkContext(conf=conf)

def findSimilarity(x):
	tpl = eval(x)
	emptyList = []
	for i in range(0, len(tpl[1])-1):
		l = tpl[1]
		for j in range(i+1, len(l)):
			#emptyList.append((tpl[0], ((l[i][0], l[j][0]), l[i][1]*l[j][1])))
			emptyList.append((tuple(sorted((l[i][0], l[j][0]))), l[i][1]*l[j][1]))
	return emptyList
	
inverted_index = sc.textFile(inputFiles)\
.map(findSimilarity)\
.filter(lambda x: x!=[])\
.map(lambda x: map(lambda tpl: tpl, x))\
.flatMap(lambda x: x)\
.map(lambda x: (x[0], x[1]))\
.reduceByKey(lambda x, y: x+y, numPartitions=500)

inverted_index.saveAsTextFile(outputFiles)
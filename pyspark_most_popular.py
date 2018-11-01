from pyspark import SparkContext, SparkConf
from operator import add
import re
import sys

inputFiles = sys.argv[1]
outputFiles = sys.argv[2]

conf = SparkConf().setAppName("Most Popular")
sc = SparkContext(conf=conf)

def filtering(w):
	lowered = w.encode('utf-8').lower()
	filtered =  re.sub('[~!@\[#\]$%^&*()><:;_+-=\'"/., ]', '', lowered)
	if filtered == '':
		return (filtered, 0)
	else:
		return (filtered, 1)

def rv_StopWords(w):
	lowered = w.encode('utf-8').lower()
	stop_words = ['ourselves', 'hers', 'between', 'yourself', 'but', 'again', 
	'there', 'about', 'once', 'during', 'out', 'very', 'having', 'with', 'they', 
	'own', 'an', 'be', 'some', 'for', 'do', 'its', 'yours', 'such', 'into', 'of', 
	'most', 'itself', 'other', 'off', 'is', 's', 'am', 'or', 'who', 'as', 'from', 
	'him', 'each', 'the', 'themselves', 'until', 'below', 'are', 'we', 'these', 
	'your', 'his', 'through', 'don', 'nor', 'me', 'were', 'her', 'more', 'himself', 
	'this', 'down', 'should', 'our', 'their', 'while', 'above', 'both', 'up', 'to', 
	'ours', 'had', 'she', 'all', 'no', 'when', 'at', 'any', 'before', 'them', 'same', 
	'and', 'been', 'have', 'in', 'will', 'on', 'does', 'yourselves', 'then', 'that', 
	'because', 'what', 'over', 'why', 'so', 'can', 'did', 'not', 'now', 'under', 'he', 
	'you', 'herself', 'has', 'just', 'where', 'too', 'only', 'myself', 'which', 'those', 
	'i', 'after', 'few', 'whom', 't', 'being', 'if', 'theirs', 'my', 'against', 'a', 
	'by', 'doing', 'it', 'how', 'further', 'was', 'here', 'than']
	filtered =  re.sub('[~!@#$%^&*()><:;_+-=\'"/.,]', '', lowered)
	if filtered not in stop_words:
		return True
	else:
		return False

#lines = sc.textFile("/bigd43/input_pyspark/*.txt")
#lines = sc.textFile("/cosc6339_hw2/large-dataset/*.txt")
lines = sc.textFile(inputFiles)
words = lines.flatMap(lambda line: line.split()).filter(rv_StopWords).map(filtering)
counts = words.reduceByKey(add, numPartitions=100)
rdd = sc.parallelize(counts.takeOrdered(1000, key=lambda y: -y[1]),100)
#rdd.saveAsTextFile('/bigd43/1000_most')
rdd.saveAsTextFile(outputFiles)

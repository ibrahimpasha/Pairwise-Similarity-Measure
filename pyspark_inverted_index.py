from pyspark import SparkContext, SparkConf
from operator import add
import re
from ast import literal_eval
import os
import sys

inputWC = sys.argv[1]
inputAllFiles = sys.argv[2]
outputFiles = sys.argv[3]
#inputWC = '/bigd43/1000_most/*'
#inputAllFiles = '/cosc6339_hw2/large-dataset/*.txt'
#outputFiles = '/bigd43/inverted_index'

conf = SparkConf().setAppName("Inverted Index")
sc = SparkContext(conf=conf)
#myList = sc.textFile('/bigd43/pyspark_500_output/1000_most/*').map(lambda x: eval(x)[0]).collect()
# Output of 1000 most popular words as input
myList = sc.textFile(inputWC).map(lambda x: eval(x)[0]).collect()

def filtering((fileName, count, word)):
	lowered = word.encode('utf-8').lower()
	fileName = os.path.basename(fileName)
	filtered =  re.sub('[~!@\[#\]$%^&*()><:;_+-=\'"/., ]', '', lowered)
	if filtered == '':
		return ((fileName, filtered), (count, 0))
	else:
		return ((fileName, filtered), (count, 1))
	
#Input all source files
lines = sc.wholeTextFiles(inputAllFiles)
output = lines.map(lambda (fileName, content): ((fileName, len(content.split()), content)))\
		.flatMap(lambda (fileName, count, content): map(lambda x: (fileName,count, x), content.split()))\
		.map(filtering)\
		.filter(lambda x: x[0][1] in myList)\
		.reduceByKey(lambda x, y: (x[0], x[1]+y[1]))\
		.map(lambda ((fileName, word), (total, count)): (word, (fileName, float(count)/float(total))))\
		.groupByKey(500).mapValues(list)
		
#print output.collect()
output.saveAsTextFile(outputFiles)

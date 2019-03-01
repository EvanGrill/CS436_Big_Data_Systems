from pyspark import SparkConf, SparkContext
import collections
conf = SparkConf().setMaster("local").setAppName("Ratings")
sc = SparkContext(conf = conf)

def parseLine(line):
	fields=line.split("\t")
	videoid=fields[0]
	relatedid=fields[9:]
	return(videoid,relatedid)
	
	
lines = sc.textFile("file:///Users/sk/Documents/BigData/0222/1.txt")
parsedLines=lines.map(parseLine)
print(parsedLines.take(1))


# def countOfoccurrence(line):
# 	ele=line.split(" ")
# 	return (int(fields[0]),len(ele)-1)
# 	
# 	
# pair=parsedLines.map(countOfoccurrence)
# print(pair.take(1))
from pyspark import SparkContext, SparkConf
from operator import add
import time

def main():
    sc = SparkContext("local[8]", "YTData")
    data = "./data/*"
    input = sc.textFile(data)
    start = time.time()
    results = input.map(lambda x: x.split('\t'))
    results = results.map(lambda x: [x[0],x[9:]])
    results = results.flatMap(lambda x: [[w,1] for w in x[1]])
    results = results.reduceByKey(add)
    results = results.sortBy(lambda x: x[1], ascending=False).take(10)
    end = time.time()
    for entry in results:
        print(entry[0], entry[1], sep=": ")
    count = input.count()
    print( str(count) + " videos processed in " + str(end - start) + " seconds." )

if __name__ == "__main__":
    main()

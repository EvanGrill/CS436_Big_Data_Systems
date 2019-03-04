from pyspark import SparkContext, SparkConf
from operator import add

def main():
    sc = SparkContext("local[8]", "YTData")
    data = "./data/080727"
    input = sc.textFile(data)
    results = input.map(lambda x: x.split('\t'))
    results = results.map(lambda x: [x[0],x[9:]])
    results = results.flatMap(lambda x: [[w,1] for w in x[1]])
    results = results.reduceByKey(add)
    results = results.sortBy(lambda x: x[1], ascending=False).take(10)
    for entry in results:
        print(entry[0], entry[1], sep=": ")

if __name__ == "__main__":
    main()

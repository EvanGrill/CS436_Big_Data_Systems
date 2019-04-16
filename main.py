from pyspark import SparkContext, SparkConf
from operator import add
import time

def prepData(input):
    output = input.filter(lambda x: len(x) > 12)
    output = output.map(lambda x: x.split('\t'))
    return output

def genGraph(x):
    return [(w,x[0]) for w in x[1]]

def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)

def pageRank(input):
    data = prepData(input)
    data = data.map(lambda x: [x[0],x[9:]])
    links = data.flatMap(genGraph).distinct().groupByKey().cache()
    ranks = links.map(lambda x: (x[0], 1.0))
    for i in range(5):
        contribs = links.join(ranks).flatMap(lambda x: computeContribs(x[1][0],x[1][1]))
        ranks = contribs.reduceByKey(add).mapValues(lambda x: x * 0.85 + 0.15)
    return ranks

def topCategories(input, number):
    data = prepData(input)
    data = data.map(lambda x: (x[3],1))
    #data = data.groupByKey()
    data = data.reduceByKey(add)
    output = data.sortBy(lambda x: x[1], ascending=False)
    return output.take(number)

def topRated(input, number):
    data = prepData(input)
    data = data.map(lambda x: (x[0],float(x[6])))
    data = data.sortBy(lambda x: x[1], ascending=False)
    return data.take(number)



def main():
    sc = SparkContext("local[8]", "YTData")
    data = "./data/*"
    input = sc.textFile(data)
    start = time.time()
    #print(topCategories(input, 10))
    print(topRated(input, 10))
    #ranks = pageRank(input)
    #for (link, rank) in ranks.sortBy(lambda x: x[1], ascending=False).take(10):
    #    print("%s has rank: %s." % (link, rank))

    #results = results.flatMap(lambda x: [[w,1] for w in x[1]])
    #results = results.reduceByKey(add)
    #results = results.sortBy(lambda x: x[1], ascending=False).take(10)
    end = time.time()
    #for entry in results:
    #    print(entry[0], entry[1], sep=": ")
    count = input.count()
    print( str(count) + " videos processed in " + str(end - start) + " seconds." )
    sc.stop()

if __name__ == "__main__":
    main()

from pyspark import SparkContext, SparkConf
from operator import add
import time
import locale

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

def pageRank(input, number):
    data = input.filter(lambda x: len(x) > 9)
    data = data.map(lambda x: [x[0],x[9:]])
    links = data.flatMap(genGraph).distinct().groupByKey().cache()
    ranks = links.map(lambda x: (x[0], 1.0))
    for i in range(5):
        contribs = links.join(ranks).flatMap(lambda x: computeContribs(x[1][0],x[1][1]))
        ranks = contribs.reduceByKey(add).mapValues(lambda x: x * 0.85 + 0.15)
    ranks.sortBy(lambda x: x[1], ascending=False)
    return ranks.take(number)

def topCategoriesByVideos(input, number):
    data = input.map(lambda x: (x[3],1))
    #data = data.groupByKey()
    data = data.reduceByKey(add)
    output = data.sortBy(lambda x: x[1], ascending=False)
    return output.take(number)

def topRated(input, number):
    data = input.map(lambda x: (x[0],float(x[6])))
    data = data.sortBy(lambda x: x[1], ascending=False)
    return data.take(number)

def topCategoriesByViews(input, number):
    data = input.map(lambda x: (x[3], int(x[5])))
    data = data.reduceByKey(add)
    output = data.sortBy(lambda x: x[1], ascending=False)
    return output.take(number)

#added for comments
def topComments(input,number):
    data=input.map(lambda x: (x[0], x[3], int(x[8])))
    output=data.sortBy(lambda x: x[2], ascending=False)
    return output.take(number)

def topUploaders(input, number):
    data = input.map(lambda x: (x[1], 1))
    data = data.reduceByKey(add)
    output = data.sortBy(lambda x: x[1], ascending=False)
    return output.take(number)

def main():
    locale.setlocale(locale.LC_ALL, 'en_US')
    sc = SparkContext("local[8]", "YTData")
    data = "./data_short/*"
    input = sc.textFile(data)
    input = prepData(input)
    start = time.time()
    catVideos = topCategoriesByVideos(input, 5)
    catViews = topCategoriesByViews(input, 5)
    rates = topRated(input, 10)
    #added for comments
    comments = topComments(input,10)
    uploads = topUploaders(input, 10)
    #ranks = pageRank(input, 10)
    end = time.time()
    vidCount = input.count()

    #print("Top 10 Videos by PageRank:")
    #for id, rank in ranks:
    #    print("Video:", id, "| Rank:", rank)
    #print(" ")
    print("Top 10 Videos by Rating:")
    for id, rating in rates:
        print("Video:", id, "| Rating:", rating)
    print(" ")
    print("Top 5 Categories (by Videos):")
    for cat, count in catVideos:
        print("Category: ", cat, "| Videos:", locale.format_string("%d", count, grouping=True))
    print(" ")
    print("Top 5 Categories (by Views):")
    for cat, count in catViews:
        print("Category: ", cat, "| Views:", locale.format_string("%d", count, grouping=True))
    print(" ")
    for id, cat, com in comments:
        print("Video:", id, "Category:", cat, "Comments:", com)
    print(" ")
    for user, count in uploads:
        print("User:", user, "| Uploads:", count)
    print(" ")

    print( str(vidCount) + " videos processed in " + str(end - start) + " seconds." )
    sc.stop()

if __name__ == "__main__":
    main()

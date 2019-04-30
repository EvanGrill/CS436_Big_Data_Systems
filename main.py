from pyspark import SparkContext, SparkConf
from operator import add
import time
import locale
import matplotlib.pyplot as plt
import numpy as np

def prepData(input):
    """ Filters and splits the data to be usable by other functions.

    Keyword arguments:
    input -- Fresh input data
    """
    output = input.filter(lambda x: len(x) > 12)
    output = output.map(lambda x: x.split('\t'))
    return output

def genGraph(x):
    return [(w,x[0]) for w in x[1]]

def computeContribs(links, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_links = len(links)
    for link in links:
        yield (link, rank / num_links)

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

def topCategoriesByVideos(input, number=5, sort=True, all=False):
    data = input.map(lambda x: (x[3],1))
    data = data.reduceByKey(add)
    if(sort): data = data.sortBy(lambda x: x[1], ascending=False)
    else: data = data.sortBy(lambda x: x[0], ascending=False)
    if(not all): return data.take(number)
    else: return data.collect()

def topRated(input, number):
    data = input.map(lambda x: (str(x[0]),str(x[3]),float(x[6]),str(x[7])))
    data = data.sortBy(lambda x: x[3], ascending=False)
    return data.take(number)


def topCategoriesByViews(input, number):
    data = input.map(lambda x: (str(x[3]), int(x[5])))
    data = data.reduceByKey(add)
    output = data.sortBy(lambda x: x[1], ascending=False)
    return output.take(number)

#added for comments
def topComments(input,number):
    data=input.map(lambda x: (str(x[0]), str(x[3]), int(x[8])))
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
    #data = "./data/*"
    input = sc.textFile(data)
    input = prepData(input)
    start = time.time()
    catVideos = topCategoriesByVideos(input, 5, sort=False, all=True)
    catViews = topCategoriesByViews(input, 5)
    rates = topRated(input, 10)
    comments = topComments(input,10)
    uploads = topUploaders(input, 10)
    ranks = pageRank(input, 10)
    end = time.time()
    vidCount = input.count()

    print("Top 10 Videos by PageRank:")
    for id, rank in ranks:
        print("Video:", id, "| Rank:", rank)
    print(" ")
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
    for id, cat, count in comments:
        print("Video:", id, "| Category:", cat, "| Comments:", locale.format_string("%d", count, grouping=True))
    print(" ")
    for user, count in uploads:
        print("User:", user, "| Uploads:", count)
    print(" ")

    #plt.rcdefaults()
    #fig, ax = plt.subplots()
    #y_pos = np.arange(len(catVideos))
    #chart = ax.barh(y_pos, [x[1] for x in catVideos], 0.5, color="SkyBlue")
    #ax.set_yticks(y_pos)
    #ax.set_yticklabels([x[0] for x in catVideos])
    #plt.show()

    print( str(vidCount) + " videos processed in " + str(end - start) + " seconds." )
    sc.stop()

if __name__ == "__main__":
    main()

from pyspark import SparkContext, SparkConf
from operator import add
import time
import locale
#import matplotlib.pyplot as plt
#import numpy as np

def prepData(input):
    """
    Filters and splits the data to be usable by other functions.

    Keyword arguments:
    input -- Fresh input data
    """
    output = input.filter(lambda x: len(x) > 12)
    output = output.map(lambda x: x.split('\t'))
    return output

def genGraph(x):
    """
    Outputs a list of graph links.

    Keyword Arugments:
    x -- list of type [video, list of videos to link to]
    """
    return [(w,x[0]) for w in x[1]]

def computeContribs(links, rank):
    """
    Calculates video contributions to the rank of other videos.
    
    Keyword arguments:
    links -- list of graph links
    rank -- current rank
    """
    num_links = len(links)
    for link in links:
        yield (link, rank / num_links)

def pageRank(input, number):
    """
    Calculates PageRank of Video Graph
    
    Keyword Arugments:
    input -- Sanitized YouTube data
    number -- Number of top entries to return
    """
    data = input.filter(lambda x: len(x) > 9)
    data = data.map(lambda x: [x[0],x[9:]])
    links = data.flatMap(genGraph).distinct().groupByKey().cache()
    ranks = links.map(lambda x: (x[0], 1.0))
    for i in range(5):
        contribs = links.join(ranks).flatMap(lambda x: computeContribs(x[1][0],x[1][1]))
        ranks = contribs.reduceByKey(add).mapValues(lambda x: x * 0.85 + 0.15)
    ranks.sortBy(lambda x: x[1], ascending=False, numPartitions=1)
    return ranks.take(number)

def topCategoriesByVideos(input, number=-1, sort=True):
    """
    Calculates the top categories by number of videos.

    Keyword Arguments:
    input -- Sanitized YouTube data
    number -- Number of top entries to return (-1 = all)
    sort -- True: Sort by number of videos False: Sort categories alphabetically.
    """ 
    data = input.map(lambda x: (x[3],1))
    data = data.reduceByKey(add)
    if(sort): data = data.sortBy(lambda x: x[1], ascending=False, numPartitions=1)
    else: data = data.sortBy(lambda x: x[0], ascending=False, numPartitions=1)
    if(not number == -1): return data.take(number)
    else: return data.collect()

def topRated(input, number=-1):
    """
    Calculates the top rated videos.

    Keyword Arguments:
    input -- Sanitized YouTube data
    number -- Number of top entries to return (-1 = all)
    """
    data = input.filter(lambda x: float(x[6]) == 5.0)
    data = data.map(lambda x: (x[0], int(x[7])))
    #data = input.map(lambda x: (x[0], float(x[6]), int(x[7])))
    data = data.reduceByKey(add)
    data = data.sortBy(lambda x: x[1], ascending=False, numPartitions=1)
    if(not number == -1): return data.take(number)
    else: return data.collect()


def topCategoriesByViews(input, number=-1):
    """
    Calculates the top categories by total number of views.

    Keyword Arguments:
    input -- Sanitized YouTube data
    number -- Number of top entries to return (-1 = all)
    """
    data = input.map(lambda x: (str(x[3]), int(x[5])))
    data = data.reduceByKey(add)
    data = data.sortBy(lambda x: x[1], ascending=False, numPartitions=1)
    if(not number == -1): return data.take(number)
    else: return data.collect()

def topComments(input, number=-1):
    """
    Calculates the videos with the most comments.

    Keyword Arguments:
    input -- Sanitized YouTube data
    number -- Number of top entries to return (-1 = all)
    """
    data = input.map(lambda x: (str(x[0]), int(x[8])))
    data = data.reduceByKey(add)
    data = data.sortBy(lambda x: x[1], ascending=False, numPartitions=1)
    if(not number == -1): return data.take(number)
    else: data.collect()

def topUploaders(input, number=-1):
    """
    Calculates the users with the most uploaded videos.

    Keyword Arguments:
    input -- Sanitized YouTube data
    number -- Number of top entries to return (-1 = all)
    """
    data = input.map(lambda x: (x[1], 1))
    data = data.reduceByKey(add)
    data = data.sortBy(lambda x: x[1], ascending=False, numPartitions=1)
    if(not number == -1): return data.take(number)
    else: return data.collect()

def commas(input):
    """
    Formats input as string with commas in US format.

    Keyword Arguments:
    input -- Number to be formatted
    """
    locale.setlocale(locale.LC_ALL, 'en_US')
    return locale.format_string("%d", input, grouping=True)

def main():
    sc = SparkContext("local[8]", "YTData")
    data = "./data/*"
    #data = "./data/*"
    input = sc.textFile(data)
    input = prepData(input)
    start = time.time()
    #catVideos = topCategoriesByVideos(input, 5, sort=True)
    #catViews = topCategoriesByViews(input, 5)
    #rates = topRated(input, 20)
    #comments = topComments(input,10)
    uploads = topUploaders(input, 10)
    #ranks = pageRank(input, 10)
    end = time.time()
    #vidCount = input.count()

    #print("Top 10 Videos by PageRank:")
    #for id, rank in ranks:
    #    print("Video:", id, "| Rank:", rank)
    #print(" ")
    #print("Top 10 Videos by Rating:")
    #for id, count in rates:
    #    print("Video:", id, "| Rating: 5.0 | Ratings:", commas(count))
    #print(" ")
    #print("Top 5 Categories (by Videos):")
    #for cat, count in catVideos:
    #    print("Category: ", cat, "| Videos:", commas(count))
    #print(" ")
    #print("Top 5 Categories (by Views):")
    #for cat, count in catViews:
    #    print("Category: ", cat, "| Views:", commas(count))
    #print(" ")
    #print("Top 10 Videos (by Comments):")
    #for id, count in comments:
    #    print("Video:", id, "| Comments:", commas(count))
    #print(" ")
    for user, count in uploads:
        print("User:", user, "| Uploads:", commas(count))
    print(" ")

    #plt.rcdefaults()
    #fig, ax = plt.subplots()
    #y_pos = np.arange(len(catVideos))
    #chart = ax.barh(y_pos, [x[1] for x in catVideos], 0.5, color="SkyBlue")
    #ax.set_yticks(y_pos)
    #ax.set_yticklabels([x[0] for x in catVideos])
    #plt.show()

    #print( str(vidCount) + " videos processed in " + str(end - start) + " seconds." )
    sc.stop()

if __name__ == "__main__":
    main()

from pyspark import SparkConf, SparkContext
import collections

#cofigure spark context and intialize it
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

#import file
lines = sc.textFile("/home/oleksii/spark_udemy/ml-latest/tags.csv")
#define first row
frow = lines.first()
#removing columns names
lines = lines.filter(lambda x: x != frow)
#split by coma  and define tags
tags = lines.map(lambda x: x.split(',')[2])
#count tags
result = tags.countByValue()

#sort result in asc order of a total count by tag /for desc add into sorted() third parametr reverse = True/
sortedResults = collections.OrderedDict(sorted(result.items(), key = lambda item: item[1]))
#show in console sorted result
for key, value in sortedResults.items():
    print("%s %i" % (key, value))

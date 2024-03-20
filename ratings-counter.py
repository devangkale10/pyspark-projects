import collections

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf=conf)

lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")  # Create an RDD from the file
ratings = lines.map(
    lambda x: x.split()[2]
)  # Extract ratings from each line, create new RDD
result = ratings.countByValue()  # Count the number of times each rating appears
# Will give a tuple with rating as key and count as value


sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))

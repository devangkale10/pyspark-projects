import collections

from pyspark import SparkConf, SparkContext


def parseLine(line):
    fields = line.split(",")
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)


conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf=conf)
lines = sc.textFile("file:///SparkCourse/fakefriends.csv")
rdd = lines.map(parseLine)

totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(
    lambda x, y: (x[0] + y[0], x[1] + y[1])
)
# mapValues will keep the key as it is and apply the function to the value
# Output would be (33, (390, 1)) where 33 is the key (age) and 390, 1 are the no of friends
# 1 is the no. of times the key was encountered


# reduceByKey will add the values of the same key
# Output would be (33, (390, 1)) + (33, (400, 1)) = (33, (790, 2))

# Now we have to calculate the average by dividing the total by the no. of times the key was encountered
# We can use mapValues again to apply the function to the value
# Output would be (33, 790/2) = (33, 395)
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
results = averagesByAge.collect()

# Convert avg to whole number
# results is a list of tuples

for result in results:
    result = (result[0], int(result[1]))
    print(result)

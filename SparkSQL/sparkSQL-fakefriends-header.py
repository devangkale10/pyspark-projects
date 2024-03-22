from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("FriendsByAge").getOrCreate()
lines = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv("fakefriends-header.csv")
)

friendsByAge = lines.select("age", "friends")

friendsByAge.groupBy("age").avg("age").sort("age").show()


# With a custom column name and prettier output

friendsByAge.groupBy("age").agg(
    func.round(func.avg("friends"), 2).alias("friends_avg")
).sort("age").show()

spark.stop()

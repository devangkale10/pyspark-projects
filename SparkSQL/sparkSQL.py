from pyspark.sql import Row, SparkSession

spark = SparkSession.builder.appName("sparkSQL").getOrCreate()


def mapper(line):
    fields = line.split(",")
    return Row(
        ID=int(fields[0]),
        name=str(fields[1]),
        age=int(fields[2]),
        numFriends=int(fields[3]),
    )


lines = spark.sparkContext.textFile("fakefriends.csv")
people = lines.map(mapper)

schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

for teen in teenagers.collect():
    print(teen)

spark.stop()

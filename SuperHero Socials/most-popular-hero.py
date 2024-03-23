from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType(
    [StructField("id", IntegerType(), True), StructField("name", StringType(), True)]
)

names = (
    spark.read.schema(schema)
    .option("sep", " ")
    .csv(r"C:\SparkCourse\pyspark-projects\SuperHero Socials\Marvel+Names")
)
lines = spark.read.text(
    r"C:\SparkCourse\pyspark-projects\SuperHero Socials\Marvel+Graph"
)

# Count connections by making first entry in the text as id and using func.size on remaining text, which is split by space
connections = (
    lines.withColumn("id", func.split(func.col("value"), " ")[0])
    .withColumn("connections", func.size(func.split(func.col("value"), " ")) - 1)
    .groupBy("id")
    .agg(func.sum("connections").alias("connections"))
)

mostPopular = connections.sort(func.col("connections").desc()).first()

mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name").first()

print(
    f"{mostPopularName[0]} is the most popular superhero with {mostPopular[1]} co-appearances.\n"
)

# Most obscure superheroes
minConnectionsCount = connections.agg(func.min("connections")).first()[0]
minConnections = connections.filter(func.col("connections") == minConnectionsCount)
minConnectionwithNames = minConnections.join(names, "id")
print(
    "The following characters have only " + str(minConnectionsCount) + " connection(s):"
)

minConnectionwithNames.select("name").show()
spark.stop()

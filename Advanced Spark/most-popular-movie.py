from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import IntegerType, LongType, StructField, StructType

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

# Create Schema

schema = StructType(
    [
        StructField("userID", IntegerType(), True),
        StructField("movieID", IntegerType(), True),
        StructField("rating", IntegerType(), True),
        StructField("timestamp", LongType(), True),
    ]
)
moviesDF = (
    spark.read.option("sep", "\t")
    .schema(schema)
    .csv(r"C:\SparkCourse\pyspark-projects\ml-100k\u.data")
)
topMoviesID = moviesDF.groupBy("movieID").count().orderBy(func.desc("count"))
topMoviesID.show(10)

spark.stop()

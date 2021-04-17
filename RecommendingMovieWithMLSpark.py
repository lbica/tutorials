from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import lit


def loadMovieNames():
    movieNames = {}

    with open('u.item') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1].decode('ascii', 'ignore')
    return movieNames


def parseInput(line):
    fields = line.split()
    return Row(userID=int(fields[0]), movieID=int(fields[1]), rating=float(fields[2]))


if __name__ == '__main__':
    # Create a SparkSession
    sparkSession = SparkSession.builder.appName("MovieRecs").getOrCreate()

    # Load up our Movie Id -> movie name lookup table
    movieNames = loadMovieNames()

    # Load up the raw u.data file
    lines = sparkSession.read.text('hdfs:///data/ml-100k/u.data').rdd

    # lines = sparkSession.sparkContext.textFile('data/ml-100k/u.data')

    # Convert to a RDD of Row objects with (userID, movieId, rating)
    ratingsRDD = lines.map(parseInput)

    # convert that to a dataframe and cache it
    ratings = sparkSession.createDataFrame(ratingsRDD).cache()

    # Create an ALS collaborative filtering model from the completed data set
    als = ALS(maxIter=5, regParam=0.05, userCol="userID", itemCol="movieID", ratingCol="rating")
    model = als.fit(ratings)

    # Print out ratings from user 0
    print("\nRating for user ID 0:")
    userRatings = ratings.filter("userID = 0")
    for rating in userRatings.collect():
        print(movieNames[rating['movieID']], rating['rating'])

    print("\nTop 20 recommendation for user ID 0:")

    # Find movies rated more than 100 times
    ratingCounts = ratings.groupBy("movieID").count().filter("count > 100")

    # Construct a "test" dataframe for user 0 with every movie rated more than 100 times
    popularMovies = ratingCounts.select("movieID").withColumn("userID", lit(0))

    # Run our model on that list of popular movies for userID 0
    recommendations = model.transform(popularMovies)

    # Get the top 20 movies with the highest predicted rating for this user
    topRecommendation = recommendations.sort(recommendations.prediction.desc()).take(20)

    for recommendation in topRecommendation:
        print(movieNames[recommendation['movieID']], recommendation['prediction'])

    # Stop the spark session
    sparkSession.stop()

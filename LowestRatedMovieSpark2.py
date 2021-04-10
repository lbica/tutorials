from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions


def loadMovieNames():
    movieNames = {}

    with open('u.item') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames


def parseInput(line):
    fields = line.split()
    return Row(movieID=int(fields[1]), rating=float(fields[2]))


if __name__ == '__main__':
    # Create a SparkSession
    sparkSession = SparkSession.builder.appName("PopularMovie").getOrCreate()

    # Load up our Movie Id -> movie name lookup table
    movieNames = loadMovieNames()

    # Load up the raw u.data file
    lines = sparkSession.sparkContext.textFile('hdfs:///data/ml-100k/u.data')

    # lines = sparkSession.sparkContext.textFile('data/ml-100k/u.data')

    # Convert to a RDD of Row objects with (movieId, rating)
    movies = lines.map(parseInput)

    # convert that to a dataframe
    movieDataset = sparkSession.createDataFrame(movies)

    # # Reduce to (movieId, (sumOfRatings, totalRatings))
    # ratingTotalsAndCount = movieRatings.reduceByKey(lambda movie1, movie2: (movie1[0] + movie2[0], movie1[1] + movie2[1]))

    # Map to (movieID, averageRating)
    averageRatings = movieDataset.groupBy("movieID").avg("rating")

    # Computes count of ratings for each movieID: (movieID, count)
    counts = movieDataset.groupBy("movieID").count()

    # Join the two together ( We now have movieID, avg(rating) and count columns

    # define condition for joining dataframes:
    # cond = [averageRatings.movieID == counts.movieID]

    averagesAndCounts = averageRatings.join(counts, "movieID")

    # Pull the top 10 results

    topTen = averagesAndCounts.orderBy("avg(rating)").take(10)

    # # Print them out: convert movieID's to name as we go
    for movie in topTen:
        print(movieNames[movie[0]], movie[1], movie[2])

    # Stop the spark session
    sparkSession.stop()

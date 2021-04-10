from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions
    with open('u.item') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames


def parseInput(line):
    fields = line.split()
    return Row(movieId=int(fields[1]), rating= float(fields[2]))


if __name__ == '__main__':
    # Create a SparkSession
    sparkSession = SparkSession.builder.appName("PopularMovie").getOrCreate()

    # Load up our Movie Id -> movie name lookup table
    movieNames = loadMovieNames()

    # Load up the raw u.data file
    lines = sparkSession.sparkContext.textFile('hdfs:///data/ml-100k/u.data')

    # Convert to a RDD of Row objects with (movieId, rating)
    movies = lines.map(parseInput)

    # convert that to a dataframe
    movieDataset = sparkSession.createDataFrame(movies)

    # # Reduce to (movieId, (sumOfRatings, totalRatings))
    # ratingTotalsAndCount = movieRatings.reduceByKey(lambda movie1, movie2: (movie1[0] + movie2[0], movie1[1] + movie2[1]))

    # Map to (movieID, averageRating)
    averageRatings = movieDataset.groupBy(lambda a: functions.mean(a))

    # Sort by Average Ratings
    # sortedMovies = averageRatings.sortBy(lambda x: x[1])
    #
    # # Take the Top 10 results
    # results = sortedMovies.take(10)
    #
    # # Print them out
    # for result in results:
    #     print(movieNames[result[0]], result[1])

from pyspark import SparkConf, SparkContext



def loadMovieNames():
    movieNames = {}

    with open('data/ml-100k/u.item') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames


def parseInput(line):
    fields = line.split()
    return (int(fields[1]), (float(fields[2]), 1.0))


if __name__ == '__main__':
    # The main scripts - Create our SparkContext
    conf = SparkConf().setAppName("WorstMovie")
    sc = SparkContext(conf=conf)

    # Load up our Movie Id -> movie name lookup table
    movieNames = loadMovieNames()

    # Load up the raw u.data file
    lines = sc.textFile('hdfs:///data/ml-100k/u.data')

    # Convert to (movieId, (rating, 1.0))
    movieRatings = lines.map(parseInput)


    # Reduce to (movieId, (sumOfRatings, totalRatings))
    ratingTotalsAndCount = movieRatings.reduceByKey(lambda movie1, movie2: (movie1[0] + movie2[0], movie1[1] + movie2[1]))

    # Map to (movieID, averageRating)
    averageRatings = ratingTotalsAndCount.mapValues(lambda totalAndCount: totalAndCount[0]/totalAndCount[0])

    # Sort by Average Ratings
    sortedMovies = averageRatings.sortBy(lambda x: x[1])

    # Take the Top 10 results
    results = sortedMovies.take(10)

    # Print them out
    for result in results:
        print(movieNames[result[0]], result[1])

# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from mrjob.job import MRJob
from mrjob.step import MRStep


class TopMovies(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_rattings,
                   reducer=self.reducer_count_ratings),
            MRStep(reducer=self.reducer_sorted_output)
        ]

    def mapper_get_rattings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    def reducer_count_ratings(self, key, values):
        yield str(sum(values)).zfill(5), key

    def reducer_sorted_output(self, count, movies):
        for movie in movies:
            yield movie, count



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    TopMovies.run()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/

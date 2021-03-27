# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from mrjob.job import MRJob
from mrjob.step import MRStep


class RatingsBreakdowns(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratting,
                   reducer=self.reduce_count_ratings)
        ]

    def mapper_get_ratting(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield rating, 1

    def reduce_count_ratings(self, key, values):
        yield key, sum(values)




# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    RatingsBreakdowns.run()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/

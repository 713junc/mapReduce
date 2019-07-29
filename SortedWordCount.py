from mrjob.job import MRJob
from mrjob.step import MRStep # for doing a multi-step job
import re # regular expression

WORD_REGEXP = re.compile(r"[\w']+")

class MRSortedWordCount(MRJob):

	# Let mapReduce know what order to run everything
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,combiner=self.combiner_count_words, reducer=self.reducer_count_words),
            MRStep(reducer = self.reducer_sorted_result)
        ]

    def mapper_get_words(self, _, line):
        words = WORD_REGEXP.findall(line)
        for word in words:
            yield (word.lower(), 1)

    def combiner_count_words(self, word, values):
        yield (word, sum(values))

    def reducer_count_words(self, word, values):
        yield None, (sum(values), word)

    # Output in ascending order
    # word_count is like a list of pairs like (40, 'few')
    def reducer_sorted_result(self, _, word_count):
        for word in sorted(word_count, reverse=False):
            yield word

if __name__ == '__main__':
    MRSortedWordCount.run()


"""
Input Data: Text from a book
1. Mapper is going to break up every line into individual words.
2. Combiner is going to sum the words.
3. Reducer1 will have a special output format.
4. Reducer2 will print all the results in ascending order.


# MRStep object specifies a mapper, combiner and reducer.
  All three are optiional, but must have at least one.

# A Combiner in Hadoop is a mini reducer that performs the local reduce task.
  Many MapReduce jobs are limited by the network bandwidth available on the cluster, so the combiner minimizes the data transferred between map and reduce tasks.
"""

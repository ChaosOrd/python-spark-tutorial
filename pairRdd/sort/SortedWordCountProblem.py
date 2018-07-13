import sys

sys.path.insert(0, '.')
from commons.Utils import Utils


from pyspark import SparkContext, SparkConf


if __name__ == "__main__":
    '''
    Create a Spark program to read the an article from in/word_count.text,
    output the number of occurrence of each word in descending order.

    Sample output:

    apple : 200
    shoes : 193
    bag : 176
    ...

    '''
    conf = SparkConf().setAppName("averageHousePriceSolution").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    lines = sc.textFile("in/word_count.text")
    words = lines.flatMap(lambda line: line.split(" "))
    words = words.map(lambda w: (w, 1))
    words = words.reduceByKey(lambda c1, c2: c1 + c2)
    words = words.sortBy(lambda w: w[1], ascending=False)
    words_collected = words.collect()
    for word in words_collected:
        print(f'{word[0]}: {word[1]}')

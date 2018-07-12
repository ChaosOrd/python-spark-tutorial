import sys
from pyspark import SparkContext, SparkConf


def split_by_space(line: str):
    return [num for num in line.split('\t') if num != '']

if __name__ == "__main__":

    '''
    Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
    print the sum of those numbers to console.
    Each row of the input file contains 10 prime numbers separated by spaces.
    '''
    conf = SparkConf().setAppName("sumOfPrimeNumbers").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    numbers = sc.textFile("in/prime_nums.text").flatMap(split_by_space)
    numbers = numbers.map(lambda x: int(x))
    sum_ = numbers.reduce(lambda x, y: x + y)
    print("sum is :{}".format(sum_))

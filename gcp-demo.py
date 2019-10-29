#!/usr/bin/env python

from pyspark import SparkContext

sc = SparkContext("local")

input_file = sc.textFile("/optus-demo/decline-and-fall.txt")
word_counts = input_file.flatMap(lambda line: line.split(" ")) \
                .map(lambda word: (word, 1)) \
                .reduceByKey(lambda x, y: x + y) \
                .sortByKey()

word_counts.saveAsTextFile("/optus-demo/fall-count")

print("File was saved ...")

word_list = word_counts.collect()

print("There were {} unique words in the file".format(len(word_list)))



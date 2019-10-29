#!/usr/bin/env python

from pyspark import SparkContext

sc = SparkContext("local")

input_file = sc.textFile("hdfs://data/moby-dick.txt")
word_counts = input_file.flatMap(lambda line: line.split(" ")) \
                .map(lambda word: (word, 1)) \
                .reduceByKey(lambda x, y: x + y) \
                .map(lambda k, v: v, k) \
                .sortByKey()

word_counts.saveAsTextFile("hdfs://data/moby-word-count.txt")


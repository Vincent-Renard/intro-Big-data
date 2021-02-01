import sys
import pyspark
import string
import operator

sc = pyspark.SparkContext()

sc.setLogLevel("ERROR")

dirName = sys.argv[1]

occurs = sc.textFile(dirName) \
    .flatMap(lambda line: line.split(" ")) \
    .map(lambda word: word.translate(str.maketrans("", "", string.punctuation))) \
    .map(str.lower) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(operator.add) \
    .sortByKey() \
    .map(lambda occur: "%s\t%d\n" % (occur[0], occur[1])) \
    .collect()

with open("output-word-count.txt", "w") as outputFile:
    outputFile.writelines(occurs)

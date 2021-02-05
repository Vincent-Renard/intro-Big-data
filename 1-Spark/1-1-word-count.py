from sys import argv
from re import sub
import pyspark
from pyspark.sql.session import SparkSession

if len(argv) < 2:
    print("J'ai besoin d'un répertoire d'entrée")
    print("Usage : spark-submit 1-1-word-count.py <dir>")
    exit()

def clean_word(word):
    return sub(r"[^a-z]", "", word)

sc = pyspark.SparkContext()
sc.setLogLevel("ERROR")
spark = SparkSession(sc)

dirr = argv[1]

pool = sc.textFile(dirr)
words = pool.flatMap(lambda line: line.lower().split())
lower_words = words.map(clean_word)
word_count = lower_words.countByValue()

for w, c in sorted(word_count.items()):
    print(w + "\t" + str(c))

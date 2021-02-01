
from sys import argv
from os import listdir ,sep
from re import sub
import pyspark
from pyspark.sql.session import SparkSession

def clean_word(word):

    return sub(r"[^a-z]", "", word)


sc = pyspark.SparkContext()
sc.setLogLevel("ERROR")
spark = SparkSession(sc)

if len(argv) <2:
    print("J'ai besoin d'un repertoire d'entrée \nUsage : pyspark moi.py dir")
    exit()


dirr = argv[1]


pool = sc.wholeTextFiles(dirr)

files = pool.map(lambda content : content[1] )

words=files.flatMap(lambda lines: lines.lower().split())

lower_words = words.map(clean_word)

word_count = lower_words.countByValue()

for w, c in sorted(word_count.items()):
    print(w,"\t",c)
    pass
#df = spark.createDataFrame(lines, ["word", "count"])


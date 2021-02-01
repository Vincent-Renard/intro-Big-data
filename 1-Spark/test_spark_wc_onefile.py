
from sys import argv
import pyspark
from pyspark.ml.fpm import FPGrowth
from pyspark.sql.session import SparkSession

def clean_word(word):
    return "".join(car for car in word.lower() if car.isalpha())



sc = pyspark.SparkContext()
sc.setLogLevel("ERROR")
spark = SparkSession(sc)
if len(argv) <2:
    print("J'ai besoin d'un fichier d'entrée \nUsage : pyspark moi.py fichier")



#fichier_mots = argv[1]

fichier_mots = "semaine2-fichiers/input-word-count/file1.txt"
words = sc.textFile(fichier_mots).flatMap(lambda lines: lines.split(" "))




lower_words = words.map(clean_word)


word_count = lower_words.countByValue()

for w,c in word_count.items():
    print(w,"\t",c)
#df = spark.createDataFrame(lines, ["word", "count"])

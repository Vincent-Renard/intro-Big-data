
from sys import argv

import pyspark
from pyspark.sql.session import SparkSession


if len(argv) < 3:
    print("J'ai besoin de deux fichiers d'entrée \nUsage : pyspark moi.py fichier1 fichier2")
    exit()

fichier1, fichier2 = argv[1], argv[2]

sc = pyspark.SparkContext()
sc.setLogLevel("ERROR")

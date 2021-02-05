from sys import argv
from pyspark.mllib.linalg.distributed import MatrixEntry, CoordinateMatrix

import pyspark
from pyspark.sql.session import SparkSession


if len(argv) < 3:
    print("J'ai besoin de deux fichiers d'entrée \nUsage : pyspark moi.py fichier1 fichier2")
    exit()

fichier_m, fichier_n = argv[1], argv[2]

sc = pyspark.SparkContext()
spark = SparkSession(sc)
sc.setLogLevel("ERROR")

def get_matrix_from_filename(filename):
    entries = sc.textFile(filename) \
        .map(lambda line: line.split("|")[1:]) \
        .map(lambda quatuor: [int(x) for x in quatuor]) \
        .map(lambda quatuor: MatrixEntry(quatuor[0] - 1, quatuor[1] - 1, quatuor[2]))
    return CoordinateMatrix(entries).toBlockMatrix()

def print_values(row):
    for j, value in enumerate(row.vector):
        print("%d %d   %d" % (row.index + 1, j + 1, value))

matrix_m = get_matrix_from_filename(fichier_m)
matrix_n = get_matrix_from_filename(fichier_n)

matrix_m.multiply(matrix_n) \
    .toIndexedRowMatrix() \
    .rows \
    .foreach(print_values)

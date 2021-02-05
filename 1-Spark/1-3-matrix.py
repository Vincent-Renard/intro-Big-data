from sys import argv
import pyspark
from pyspark.sql.session import SparkSession
import itertools

if len(argv) < 3:
    print("J'ai besoin de deux fichiers d'entrée")
    print("Usage : pyspark 1-3-matrix.py <fichier1> <fichier2>")
    exit()

fichier_m, fichier_n = argv[1], argv[2]

sc = pyspark.SparkContext()
sc.setLogLevel("ERROR")
spark = SparkSession(sc)

def get_matrix_from_filename(filename):
    coords = sc.textFile(filename) \
        .map(lambda line: line.split("|")[1:]) \
        .map(lambda quatuor: [int(x) for x in quatuor]) \
        .map(lambda quatuor: ((quatuor[0], quatuor[1]), quatuor[2])) \
        .collectAsMap()
    rows = max(cell[0] for cell in coords.keys())
    cols = max(cell[1] for cell in coords.keys())
    return (rows, cols, coords)

matrix_m = get_matrix_from_filename(fichier_m)
matrix_n = get_matrix_from_filename(fichier_n)

print(matrix_m)
print(matrix_n)

res = sc.parallelize(itertools.product(range(1, matrix_m[0] + 1), range(1, matrix_n[1] + 1))) \
    .map(lambda pair: ((pair[0], pair[1]), sum(matrix_m[2][(pair[0], i)] * matrix_n[2][(i, pair[1])] for i in range(1, matrix_m[1] + 1)))) \
    .collectAsMap()

for (x, y), value in res.items():
    print(x, y, " ", value)

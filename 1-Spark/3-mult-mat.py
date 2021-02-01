
from sys import argv

import pyspark


if len(argv) < 3:
    print("J'ai besoin de deux fichiers d'entrée \nUsage : pyspark moi.py fichier1 fichier2")
    exit()

fichier1, fichier2 = argv[1], argv[2]

sc = pyspark.SparkContext()
sc.setLogLevel("ERROR")

matrice_m,matrice_n = sc.textFile(fichier1),sc.textFile(fichier2)

def file_to_stream(fileName):
    return sc.textFile(fileName) \
        .map(lambda line: line.split('|')[1:]) \
        .map(lambda l: [int(x) for x in l]) \
        .map(lambda quatuor: ((quatuor[0], quatuor[1]), quatuor[2]))


matrice_m, matrice_n = file_to_stream(fichier1), file_to_stream(fichier2)

# tester si matrices compatibles
nb_rows_m = matrice_m.map(lambda entry: int(entry[0][0])).max()
nb_cols_m = matrice_m.map(lambda entry: int(entry[0][1])).max()

nb_rows_n = matrice_n.map(lambda entry: int(entry[0][0])).max()
nb_cols_n = matrice_n.map(lambda entry: int(entry[0][1])).max()

print(nb_rows_m, nb_rows_n, nb_cols_m, nb_cols_n)

# organiser matrices 
if nb_rows_m == nb_cols_n:
    matrice_a, matrice_b = matrice_m, matrice_n
elif nb_rows_n == nb_cols_m:
    matrice_a, matrice_b = matrice_n, matrice_m
else:
    print("Matrices non compatibles")
    exit()

# calculer

#mat_a
#mat_b = inv rows cols matrice_b 

#st = stream(mat_a, mat_b).map(mult)
#st.map.sum()



# afficher
print(matrice_c)

# sauver dans un fichier

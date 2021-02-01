
from sys import argv

import pyspark
from pyspark.sql.session import SparkSession


if len(argv) < 2:
    print("J'ai besoin d'un fichier d'entrée \nUsage : pyspark moi.py fichier.edgelist")
    exit()

fichier = argv[1]

sc = pyspark.SparkContext()
sc.setLogLevel("ERROR")

net = sc.textFile(fichier)

size = net.count()

has_at_least_one_follower = net.map(lambda arc: arc.split(" ")[0]).distinct().count()

follow_at_least_one = net.map(lambda arc: arc.split(" ")[1]).distinct().count()

pairs = net.map(lambda arc: (arc.split(" ")[0],arc)).groupByKey().mapValues( lambda adj_list : len(adj_list) )
rich, max_followers_per_user = pairs.max(key=lambda pair: pair[1])
poor, min_followers_per_user = pairs.min(key=lambda pair: pair[1])


print("Nb total de relations friend/follower : {}".format(size))
print("Nb utilisateurs qui ont au moins un follower : {}".format(has_at_least_one_follower))
print("Nb utilisateurs qui suivent au moins qqn : {}".format(follow_at_least_one))
print("Nb max de followers par utilisateur: {} exemple {}".format(max_followers_per_user,rich))
print("Nb min de followers par utilisateur : {} exemple {}".format(
    min_followers_per_user,poor))

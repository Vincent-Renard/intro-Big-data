from sys import argv
import operator
import pyspark
from pyspark.sql.session import SparkSession

if len(argv) < 2:
    print("J'ai besoin d'un fichier d'entrée")
    print("Usage : spark-submit 1-2-twitter.py <fichier.edgelist>")
    exit()

fichier = argv[1]

sc = pyspark.SparkContext()
sc.setLogLevel("ERROR")
spark = SparkSession(sc)

net = sc.textFile(fichier)

size = net.count()

arcs = net.map(lambda arc: arc.split(" "))

has_at_least_one_follower = arcs.map(lambda arc: arc[0]).distinct().count()

follow_at_least_one = arcs.map(lambda arc: arc[1]).distinct().count()

pairs = arcs.map(lambda arc: (arc[0], 1)).reduceByKey(operator.add)
rich, max_followers_per_user = pairs.max(key=lambda pair: pair[1])
poor, min_followers_per_user = pairs.min(key=lambda pair: pair[1])


print("Nb total de relations friend/follower : {}".format(size))
print("Nb utilisateurs qui ont au moins un follower : {}".format(has_at_least_one_follower))
print("Nb utilisateurs qui suivent au moins qqn : {}".format(follow_at_least_one))
print("Nb max de followers par utilisateur : {} exemple {}".format(max_followers_per_user,rich))
print("Nb min de followers par utilisateur : {} exemple {}".format(min_followers_per_user,poor))

import itertools
import time
from random import randint, sample
from functools import lru_cache

import pyspark
from pyspark.sql.session import SparkSession
from pyspark.ml.fpm import FPGrowth
import matplotlib.pyplot as plt

# Générateur
def generateur(nb_transactions, nb_items, intervalle):
    min_items, max_items = intervalle
    return [
        [
            "p" + str(i)
            for i in sample(
                range(nb_items),
                randint(min_items, max_items)
            )
        ]
        for _ in range(nb_transactions)
    ]

# Frequent Itemset Generation en Python
def fig_python(transactions, minsup, verbose=False):
    T = [sorted(t) for t in transactions]
    N = len(T)

    @lru_cache(maxsize=None)
    def o(X):
        return sum(1 for t in T if set(X) <= set(t))

    def apriori_gen(I, k):
        # Generation
        items = (
            tuple(sorted(set().union(A, B)))
            for A, B in itertools.combinations(I, 2)
            if all(
                A[i] == B[i]
                for i in range(k - 2)
            ) and A[k - 1 - 1] != B[k - 1 - 1]
        )
        # Pruning
        return set(
            item
            for item in items
            if all(
                o(tuple(x for j, x in enumerate(item) if j != i)) >= N * minsup
                for i in range(len(item))
            )
        )

    def print_itemsets(k, itemsets):
        print(str(k) + "-itemsets")
        for item in sorted(itemsets):
            print(", ".join(item).ljust(50), o(item), "F" if o(item) >= N * minsup else "I")

    I = {(item,) for transaction in transactions for item in transaction}

    k = 1
    if verbose:
        print_itemsets(k, I)
    F = [{i for i in I if o(i) >= N * minsup}]

    while len(F[k - 1]) > 0:
        k += 1
        C = apriori_gen(F[k - 1 - 1], k)
        if verbose:
            print("=" * 54)
            print_itemsets(k, C)
        F.append({c for c in C if o(c) >= N * minsup})

    return set().union(*F)

sc = pyspark.SparkContext()
sc.setLogLevel("ERROR")
spark = SparkSession(sc)

# Frequent Itemset Generation en Spark
def fig_spark(transactions, minsup, verbose=False):
    rdd = sc.parallelize(transactions) \
        .map(lambda x: (0, x))
    df = spark.createDataFrame(rdd, ["id", "items"])
    FPGrowth(minSupport=minsup).fit(df)

def benchmark(minsup, nb_tentatives, nb_transactions_list):
    # ///
    # Initialisation de Spark pour éviter que son temps d'initialisation s'ajoute
    fig_spark(generateur(1, 1, (1, 1)), 0)
    # ///

    times_python = []
    times_spark = []
    for nb_transactions in nb_transactions_list:
        somme_python = 0
        somme_spark = 0
        for _ in range(nb_tentatives):
            test = generateur(nb_transactions, 25, (1, 5))

            begin_python = time.time()
            fig_python(test, minsup)
            somme_python += time.time() - begin_python

            begin_spark = time.time()
            fig_spark(test, minsup)
            somme_spark += time.time() - begin_spark
        times_python.append(somme_python / nb_tentatives)
        times_spark.append(somme_spark / nb_tentatives)
    plt.plot(nb_transactions_list, times_python)
    plt.plot(nb_transactions_list, times_spark)
    plt.legend(["Python", "Spark"])
    plt.show()

transactions = [
    {"Bread", "Milk"},
    {"Bread", "Diapers", "Beer", "Eggs"},
    {"Milk", "Diapers", "Beer", "Coke"},
    {"Bread", "Milk", "Diapers", "Beer"},
    {"Bread", "Milk", "Diapers", "Coke"},
]

Ta = [
    ["a", "b", "d", "e"],
    ["b", "c", "d"],
    ["a", "b", "d", "e"],
    ["a", "c", "d","e"],
    ["b", "c", "d", "e"],
    ["b", "d", "e"],
    ["c", "d"],
    ["a", "b", "c"],
    ["a", "d", "e"],
    ["b", "d"]
]

Tb = [
    ["b", "c", "d"],
    ["a", "b", "c", "d", "e"],
    ["a", "b", "c", "e"],
    ["a", "b","d", "e"],
    ["b", "c", "e"],
    ["a", "b", "d", "e"]
]

benchmark(0.05, 10, [1000, 2000, 3000])

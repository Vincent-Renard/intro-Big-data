import itertools
import random
import time
import pyspark
from pyspark.sql.session import SparkSession
from pyspark.ml.fpm import FPGrowth
import matplotlib.pyplot as plt
from functools import cache

def generateur(nb_transactions, nb_items, intervalle):
    min_items, max_items = intervalle
    return [
        [
            "p" + str(i)
            for i in random.sample(
                range(nb_items),
                random.randint(min_items, max_items)
            )
        ]
        for _ in range(nb_transactions)
    ]

def fig(transactions, minsup, verbose=False):
    T = [sorted(t) for t in transactions]
    N = len(T)

    @cache
    def o(X):
        return sum(1 for t in T if set(X) <= set(t))

    def apriori_gen(I, k):
        # Generation
        items = (
            tuple(sorted(set(A) | set(B)))
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

    I = {(item,) for t in transactions for item in t}

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

    union = set().union(*F)
    return union


transactions = [
    {"Bread", "Milk"},
    {"Bread", "Diapers", "Beer", "Eggs"},
    {"Milk", "Diapers", "Beer", "Coke"},
    {"Bread", "Milk", "Diapers", "Beer"},
    {"Bread", "Milk", "Diapers", "Coke"},
]

ta = [["a", "b", "d", "e"], ["b", "c", "d"], ["a", "b", "d", "e"], ["a", "c", "d","e"], ["b", "c", "d", "e"], ["b", "d", "e"], ["c", "d"], ["a", "b", "c"], ["a", "d", "e"],["b", "d"]]
tb = [["b", "c", "d"], ["a", "b", "c", "d", "e"], ["a", "b", "c", "e"], ["a", "b","d", "e"], ["b", "c", "e"], ["a", "b", "d", "e"]]

#print("TRANSACTIONS")
#fig(transactions, 0.6, True)
#print("\nTA")
#fig(ta, 0.3, True)
#print("\nTB")
#fig(tb, 0.5, True)




sc = pyspark.SparkContext()
sc.setLogLevel("ERROR")
spark = SparkSession(sc)

def spark_fig(transactions, minsup, verbose=False):
    rdd = sc.parallelize(transactions) \
        .map(lambda x: (0, x))
    df = spark.createDataFrame(rdd, ["id", "items"])
    FPGrowth(minSupport=minsup).fit(df)

minsup = 0.05
nb_tentatives = 1
times_fig = []
times_spark = []
nb_transactions_list = [1000, 2000, 3000]
for nb_transactions in nb_transactions_list:
    somme_fig = 0
    somme_spark = 0
    for i in range(nb_tentatives):
        test = generateur(nb_transactions, 50, (5, 20))

        begin_fig = time.time()
        fig(test, minsup)
        somme_fig += time.time() - begin_fig

        begin_spark = time.time()
        spark_fig(test, minsup)
        somme_spark += time.time() - begin_spark
    times_fig.append(somme_fig / nb_tentatives)
    times_spark.append(somme_spark / nb_tentatives)

plt.plot(nb_transactions_list, times_fig)
plt.plot(nb_transactions_list, times_spark)
plt.legend(["PYTHON", "SPARK"])
plt.show()
plt.show()

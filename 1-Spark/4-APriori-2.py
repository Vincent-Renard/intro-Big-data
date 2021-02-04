import itertools
from functools import cache

def fig(transactions, minsup, verbose=False):
    T = [sorted(t) for t in transactions]

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
                o(tuple(x for j, x in enumerate(item) if j != i)) >= minsup
                for i in range(len(item))
            )
        )

    I = {items for t in transactions for items in t}
    if verbose:
        print("Items :", I)

    k = 1
    F = [{(i,) for i in I if o((i,)) >= minsup}]
    if verbose:
        print("F1 :", F[k - 1])

    while len(F[k - 1]) > 0:
        k += 1
        C = apriori_gen(F[k - 1 - 1], k)
        F.append({c for c in C if o(c) >= minsup})
        if verbose:
            print("F" + str(k) + " :", F[k - 1])

    union = set().union(*F)
    if verbose:
        print("FIG :", union)
    return union


transactions = [
    {"Bread", "Milk"},
    {"Bread", "Diapers", "Beer", "Eggs"},
    {"Milk", "Diapers", "Beer", "Coke"},
    {"Bread", "Milk", "Diapers", "Beer"},
    {"Bread", "Milk", "Diapers", "Coke"},
]

fig(transactions, 3, True)

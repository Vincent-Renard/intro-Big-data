import itertools

def apriori_gen(I, k):
    return {
        tuple(sorted(set(A) | set(B)))
        for A, B in itertools.combinations(I, 2)
        if all(A[i] == B[i] for i in range(k - 2)) and A[k - 1 - 1] != B[k - 1 - 1]
    }

def fig(transactions, minsup, verbose=False):
    T = [sorted(t) for t in transactions]
    print(T)
    def o(X):
        return sum(1 for t in T if set(X) <= set(t))

    I = {i for t in transactions for i in t}
    print(I)
    N = 1 # ?????

    k = 1
    F = [{(i,) for i in I if o({i}) >= N * minsup}]
    print(F[0])
    while len(F[k - 1]) > 0:
        k += 1
        C = apriori_gen(F[k - 1 - 1], k)
        F.append({c for c in C if o(c) >= N * minsup})
    print(F)
    return F


transactions = [
    {"Bread", "Milk"},
    {"Bread", "Diapers", "Beer", "Eggs"},
    {"Milk", "Diapers", "Beer", "Coke"},
    {"Bread", "Milk", "Diapers", "Beer"},
    {"Bread", "Milk", "Diapers", "Coke"},
]

fig(transactions, 3)

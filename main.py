#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# STUDENT : ARNAULT DERIEUX O2151186

import sys
import time
import random
import pyspark
from pyspark.sql.session import SparkSession
from pyspark.ml.fpm import FPGrowth
from pyspark.mllib.linalg.distributed import RowMatrix,CoordinateMatrix,MatrixEntry
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from pyspark import SparkConf
from pyspark.context import SparkContext

# Datas
# (supportMin,[collectionTransactions])
data1 = (0.6,[["Bread", "Milk"], ["Bread", "Diapers", "Beer", "Eggs"],
            ["Milk", "Diapers", "Beer", "Coke"], ["Bread", "Milk", "Diapers", "Beer"], ["Bread", "Milk",
            "Diapers", "Coke"]])
data2 = (0.3,[["a", "b", "d", "e"], ["b", "c", "d"], ["a", "b", "d", "e"], ["a", "c", "d",
            "e"], ["b", "c", "d", "e"], ["b", "d", "e"], ["c", "d"], ["a", "b", "c"], ["a", "d", "e"],
            ["b", "d"]])
data3 = (0.5,[["b", "c", "d"], ["a", "b", "c", "d", "e"], ["a", "b", "c", "e"], ["a", "b",
            "d", "e"], ["b", "c", "e"], ["a", "b", "d", "e"]])

# frequentItemsetGeneration
# Main function as in the book or almost
# param :
#   @_collectionTransactionsInitiale : list of list - initial collection of transactions
#   @_supportMin : number - supportMin
#   @_verbose : boolean - print or not
# return :
#   float - time of execution
def frequentItemsetGeneration(_collectionTransactionsInitiale, _supportMin,_verbose=False):
    _N_support = _supportMin * len(_collectionTransactionsInitiale)
    # printItemset
    # Used to format print
    # param :
    #   @k : number - the indice of itemset
    #   @_collectionToPrint : itemset
    def printItemset(k,_collectionToPrint):
        # listTostr
        # Used to format a list of string for the print
        def listTostr(_list):
            _res = ""
            _cptTampon = 1
            for _transaction in _list:
                _res+=str(_transaction)
                if(_cptTampon!=len(_list)):
                    _res+=","
            return _res
        if(_verbose):# Only if verbose
            if(_collectionToPrint != []):
                print(str(k)+"-itemsets")
            for e in _collectionToPrint:
                if(o(e)>=_N_support):
                    print(listTostr(e)+"\t"+str(o(e))+" F")
                else:
                    print(listTostr(e)+"\t"+str(o(e))+" I")
            if(_collectionToPrint != []):
                print("==================")
    #============================================================
    # o
    # Used to calcul the count of an element inside the initial collection of transactions
    # param :
    #   @element : the element to calcul the count
    def o(element):
        # containsDeep
        # Short function for a list contains inside list
        def containsDeep(_data,_list):
            for _tamp in _list:
                if _tamp not in _data:
                    return False
            return True
        #========================================================
        _res = 0
        for e in _collectionTransactionsInitiale:
            if(containsDeep(e,element)):
                _res+=1
        return _res
    #============================================================
    # getItemset1
    # Used to calcul the first itemset (k1)
    def getItemset1():
        _res = []
        _listForPrint = [];
        for _ in _collectionTransactionsInitiale:
            for data in _:
                if [data] not in _res and o([data])>=_N_support:
                    _res.append([data])
                if [data] not in _listForPrint:
                    _listForPrint.append([data])
        printItemset(1,sorted(_listForPrint))
        return sorted(_res)
    #============================================================
    # aprioriGen
    # As in the book or almost
    # param :
    #   @element : current element
    #   @k : current itemset deepth
    def aprioriGen(element,k):
        # FIRST PART - GENERATION
        _res_unpruned = []
        _tampon_unpruned = [sorted(x) for x in element]
        for _a in _tampon_unpruned:
            for _b in _tampon_unpruned:
                _condiOK = True
                for i in range(0,k-2):
                    if(_a[i]!=_b[i]):
                        _condiOK=False
                        break;
                if(_condiOK and _a[k-2]!=_b[k-2]):
                    _tampon_sorted = sorted(list(set(_a + _b)))
                    if(_tampon_sorted not in _res_unpruned):
                        _res_unpruned.append(_tampon_sorted)
        printItemset(k,sorted(_res_unpruned))
        #====================================================
        # SECOND PART - PRUNING
        _res_pruned = []
        for _item in _res_unpruned:
            _frequent = True
            for i in range(len(_item)):
                _tampon = []
                for j in range(len(_item)):
                    if j != i:
                        _tampon.append(_item[j])
                if o(_tampon) < _N_support:
                    _frequent = False
                    break
            if _frequent:
                _res_pruned.append(_item)
        return _res_pruned
    #=======================================================
    _start = time.time()
    k = 1
    F_k = [getItemset1()]
    while (F_k[k-1] != []):
        k+=1
        C_k = aprioriGen(F_k[len(F_k)-1], k)
        F_k.append([x for x in C_k if o(x)>=_N_support])
        # Not needed to update the o() -> already handled (dynamic function)
    # Postrocessing
    _res_list_merged = []
    for _ in F_k:
        _res_list_merged.append(_)
    _end = time.time()
    if(_verbose):
        print("Final itemset : ",_res_list_merged)
    return _end - _start # Return the time
#============================================================

sc = pyspark.SparkContext()
spark = SparkSession(sc)
sc.setLogLevel("ERROR")

# Function
def sparkImpl(_minsup,_collectionTransactions):
    rdd = sc.parallelize(_collectionTransactions)
    rdd = rdd.map(lambda x : (0, x))
    df = spark.createDataFrame(rdd, ["id", "items"])
    _start = time.time()
    FPGrowth(minSupport=_minsup).fit(df)
    _end = time.time()
    return _end - _start # Return the time

# generatorTransactions
# Same as requested
# param :
#   @_nbTransactions : number - number of wanted transactions
#   @_nbItems : number - number of items inside the data
#   @_widthStart : number
#   @_widthEnd : number
# return :
#   collection of transactions
def generatorTransactions(_nbTransactions,_nbItems,_widthStart,_widthEnd):
    if(_widthStart<_widthEnd and _nbItems>=_widthEnd):
        _setItems = ["p"+str(_cpt) for _cpt in range(0,_nbItems)]
        _res = []
        for _ in range(0,_nbTransactions):
            _nbItemsInside = random.randint(_widthStart, _widthEnd)
            _tamponInput = []
            for __ in range(0,_nbItemsInside):
                _added = False
                while(not _added):
                    _indice = random.randint(0, len(_setItems)-1)
                    if(_setItems[_indice] not in _tamponInput):
                        _tamponInput.append(_setItems[_indice])
                        _added = True
            _res.append(_tamponInput)
        return _res
    else:
        print("Error argument")
        return []

# J'ai remarqué qu'il fallait plusieur passe à Spark
# pour s'initialiser correctement... Sinon on se retrouve avec 15s
# pour le premier run, 10s pour le second etc... ?
def initSpark():
    _current_collection = generatorTransactions(250,30,1,8)
    for i in range (20):
        sparkImpl(0.05,_current_collection)
    return

# benchmark
def benchmark(_nbRun):
    initSpark()
    _minSup = 0.05
    _list_nb_transaction = [50,100,250,500,1000,1500,2000,2500,3000]
    _timers_python = []
    _timers_spark = []
    for interval in _list_nb_transaction:
        _moy_sparkl = 0
        _moy_python = 0
        for _ in range(0,_nbRun):
            _current_collection = generatorTransactions(interval,30,1,8)
            _moy_sparkl+=sparkImpl(_minSup,_current_collection)
            _moy_python+=frequentItemsetGeneration(_current_collection,_minSup,False)
        _moy_sparkl/=_nbRun
        _moy_python/=_nbRun
        _timers_python.append(_moy_python)
        _timers_spark.append(_moy_sparkl)
    df=pd.DataFrame({'x': _list_nb_transaction, 'python': _timers_python, 'spark': _timers_spark })
    plt.plot( 'x', 'python', data=df, marker='o', markerfacecolor='lightyellow', markersize=6, color='orange')
    plt.plot( 'x', 'spark', data=df, marker='o', markerfacecolor='lightyellow', markersize=6, color='skyblue')
    plt.legend()
    plt.title('Benchmark Python/Spark')
    plt.xlabel('Nombre de transactions')
    plt.ylabel('Temps(seconde)')
    plt.savefig('benchmark.png')
    return

def datasTests():
    print("\nSTART TESTS DATAS\n")
    print("########################")
    print("\nData1 - ",frequentItemsetGeneration(data1[1], data1[0],True),"\n")
    print("\nData2 - ",frequentItemsetGeneration(data2[1], data1[0],True),"\n")
    print("\nData3 - ",frequentItemsetGeneration(data3[1], data1[0],True),"\n")
    print("########################")
    print("\nEND TESTS DATAS\n")

def main(collection, supportMin,verbose=False):
    print(frequentItemsetGeneration(collection, supportMin,verbose))

def help():
    print("\nUtilisation : $> ../spark/bin/spark-submit ./main.py {verbose - optional} ")
    print("Se placer dans le répertoire courant pour le terminal\n")

if __name__ == "__main__":
    if (len(sys.argv) > 1 and sys.argv[1] in ["--help", "-help", "-h" , "help" , "h"]):
        help()
    else:
        _verbose = False
        if (len(sys.argv) > 1):
            _verbose = True
        datasTests()
        #benchmark(1)
        _current_collection = generatorTransactions(3000,30,1,8)
        _minSup = 0.05
        #frequentItemsetGeneration(_current_collection,_minSup,True)



import pyspark
from pyspark.ml.fpm import FPGrowth
from pyspark.sql.session import SparkSession


sc = pyspark.SparkContext()
sc.setLogLevel("ERROR")
spark = SparkSession(sc)

nums = sc.parallelize([1, 2, 3, 4,5,5,3,1,1,1,1])
print(nums.count())
nums2 = sc.parallelize([1, 2, 3, 4, 5,0])
print(nums2.count())

nums=nums.union(nums2)
print(nums.count())

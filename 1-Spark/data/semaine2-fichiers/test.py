import sys
import pyspark
from pyspark.ml.fpm import FPGrowth
from pyspark.sql.session import SparkSession

sc = pyspark.SparkContext()

spark = SparkSession(sc)

fichier_transactions = sys.argv[1]
minsup = float(sys.argv[2])

rdd = sc.textFile(fichier_transactions).map(lambda x:x.strip().split())

rdd = rdd.map(lambda x : (0, x))

df = spark.createDataFrame(rdd, ["id", "items"])

FPGrowth(minSupport=minsup).fit(df).freqItemsets.show()

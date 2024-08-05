from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark=SparkSession.builder\
.appName("aggregate")\
.getOrCreate()

df=spark.read.csv("stats.csv",header=True,inferSchema=True)


df.groupBy("year") \
    .agg(F.count("*").alias("count")
     ) \
    .show(truncate=False)

#sum and average from lab1
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark=SparkSession.builder\
.appName("count_and_show")\
.getOrCreate()

df=spark.read.csv("stats.csv",header=True,inferSchema=True)
df.show(3)

df.select(F.count(df.year)).show()


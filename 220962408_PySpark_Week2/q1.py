from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark=SparkSession.builder\
.appName("withColumn_and_Filter")\
.getOrCreate()

df=spark.read.csv("stats.csv",header=True,inferSchema=True)
# df.show()

df=df.withColumn('industry_code_ANZSIC',F.trim(F.col('industry_code_ANZSIC')))# remove whitespace
print(df.count())

filtered_df=df.filter((F.col('industry_code_ANZSIC').isNotNull()))# remove null rows for this col
print(filtered_df.count())


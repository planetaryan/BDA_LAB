from pyspark.sql import SparkSession
from pyspark.sql.functions import max

spark=SparkSession.builder\
.appName("Maximum")\
.getOrCreate()

data=[(1,),(2,),(8,),(4,),(5,)]
schema=["number"]

df=spark.createDataFrame(data,schema)
print("Original Dataframe")
df.show()

max_df=df.select(max("number"))
max_df.show()

# Original Dataframe
# +------+
# |number|
# +------+
# |     1|
# |     2|
# |     8|
# |     4|
# |     5|
# +------+

# +-----------+
# |max(number)|
# +-----------+
# |          8|
# +-----------+

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

spark=SparkSession.builder\
.appName("Average")\
.getOrCreate()

data=[(1,),(2,),(3,),(4,),(5,)]
schema=["number"]

df=spark.createDataFrame(data,schema)
print("Original Dataframe")
df.show()

avg_df=df.select(avg("number"))
avg_df.show()

# Original Dataframe
# +------+
# |number|
# +------+
# |     1|
# |     2|
# |     3|
# |     4|
# |     5|
# +------+

# +-----------+
# |avg(number)|
# +-----------+
# |        3.0|
# +-----------+

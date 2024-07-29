from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

spark=SparkSession.builder\
.appName("Square Integers")\
.getOrCreate()

def square(s):
    return s**2

square_udf=udf(square,IntegerType())

data=[(1,),(2,),(3,),(4,),(5,)] # each tuple is a row
schema=["number"]

df=spark.createDataFrame(data,schema)

print("Original dataframe")
df.show()

squared_df=df.withColumn("Squared",square_udf(df["number"])) # add column

print("Squared dataframe")
squared_df.show()

spark.stop()


# Original dataframe
# +------+
# |number|
# +------+
# |     1|
# |     2|
# |     3|
# |     4|
# |     5|
# +------+

# Squared dataframe
# +------+-------+
# |number|Squared|
# +------+-------+
# |     1|      1|
# |     2|      4|
# |     3|      9|
# |     4|     16|
# |     5|     25|
# +------+-------+
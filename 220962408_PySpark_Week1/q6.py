from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, stddev, min, max, count

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SummaryStatistics") \
    .getOrCreate()

# Sample data
data = [(1, "Alice", 29), (2, "Bob", 35), (3, "Charlie", 40), (4, "David", 30), (5, "Eve", 29)]
schema = ["id", "name", "age"]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Calculate summary statistics for the "age" column
summary_stats = df.select(
    mean("age").alias("Mean"),
    stddev("age").alias("Standard Deviation"),
    min("age").alias("Min"),
    max("age").alias("Max"),
    count("age").alias("Count")
)

# Show the summary statistics
summary_stats.show()


# +----+------------------+---+---+-----+                                         
# |Mean|Standard Deviation|Min|Max|Count|
# +----+------------------+---+---+-----+
# |32.6| 4.827007354458868| 29| 40|    5|
# +----+------------------+---+---+-----+

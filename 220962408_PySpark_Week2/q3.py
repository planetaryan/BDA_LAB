from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize the Spark session
spark = SparkSession.builder \
    .appName("aggregate") \
    .getOrCreate()

# Read the CSV file into a DataFrame
df = spark.read.csv("stats.csv", header=True, inferSchema=True)

# Perform aggregations: Count, Sum, and Average
df.groupBy("year") \
    .agg(
        F.count("*").alias("count"),         # Count the number of rows per year
        F.sum("employment_count").alias("total_employment"),  # Sum of employment count per year
        F.avg("employment_count").alias("avg_employment")     # Average employment count per year
    ) \
    .show(truncate=False)  # Display the results without truncating the output

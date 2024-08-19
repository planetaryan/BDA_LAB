from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("RecommendationSystem") \
    .getOrCreate()

# Define the path to your JSON dataset
file_path = "movies 1.json"  # Update with the actual path to your dataset

# Load the JSON dataset into a DataFrame
df = spark.read.json(file_path)

# Show the first few rows of the DataFrame
print("Initial DataFrame:")
df.show()

# Print the schema of the DataFrame to verify data types
print("DataFrame Schema:")
df.printSchema()

# Drop the timestamp column if it's not needed
df = df.drop("timestamp")

# Rename columns for consistency (optional)
df = df.withColumnRenamed("userId", "user") \
       .withColumnRenamed("itemId", "item") \
       .withColumnRenamed("rating", "rating")

# Show the transformed DataFrame
print("Transformed DataFrame:")
df.show()

# Stop the SparkSession
spark.stop()

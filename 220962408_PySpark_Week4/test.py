from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("CollaborativeFiltering") \
    .getOrCreate()

# Define the path to your JSON dataset
file_path = "movies 1.json"  # Update with the actual path to your dataset

# Load the JSON dataset into a DataFrame
df = spark.read.json(file_path)

# Drop the timestamp column if it's not needed
df = df.drop("timestamp")

# Rename columns for consistency with ALS model requirements
df = df.withColumnRenamed("user_id", "user") \
       .withColumnRenamed("product_id", "item") \
       .withColumnRenamed("score", "rating")

# Ensure rating column is of type double
df = df.withColumn("rating", df["rating"].cast("double"))

# Show the schema and first few rows of the DataFrame
print("Initial DataFrame:")
df.printSchema()
df.show()

# StringIndexer to convert user and item IDs to numeric indices
user_indexer = StringIndexer(inputCol="user", outputCol="userIndex")
item_indexer = StringIndexer(inputCol="item", outputCol="itemIndex")

# Fit and transform the DataFrame to add index columns
df_indexed = user_indexer.fit(df).transform(df)
df_indexed = item_indexer.fit(df_indexed).transform(df_indexed)

# Show the schema and first few rows of the DataFrame with indexed columns
print("DataFrame with Indexed Columns:")
df_indexed.printSchema()
df_indexed.show()

# Split the data into training (80%) and test (20%) sets
(training_data, test_data) = df_indexed.randomSplit([0.8, 0.2], seed=42)

# Initialize ALS model
als = ALS(
    maxIter=10,
    regParam=0.01,
    userCol="userIndex",
    itemCol="itemIndex",
    ratingCol="rating",
    coldStartStrategy="drop"  # Handle missing values in predictions
)

# Train the model
model = als.fit(training_data)

# Make predictions on the test data
predictions = model.transform(test_data)

# Show some predictions
print("Predictions:")
predictions.show()

# Evaluate the model using RMSE (Root Mean Squared Error)
evaluator = RegressionEvaluator(
    metricName="rmse",
    labelCol="rating",
    predictionCol="prediction"
)

rmse = evaluator.evaluate(predictions)
print(f"Root Mean Squared Error (RMSE): {rmse}")

# Generate top 10 recommendations for each user
user_recs = model.recommendForAllUsers(10)

# Show some recommendations
print("Top 10 Recommendations for Each User:")
user_recs.show(truncate=False)

# Generate top 10 recommendations for each item
item_recs = model.recommendForAllItems(10)

# Show some recommendations
print("Top 10 Recommendations for Each Item:")
item_recs.show(truncate=False)

# Save the trained model
model_path = "als_model"  # Update with the desired path to save the model
model.save(model_path)
print(f"Model saved to {model_path}")

# Save the test data
test_data_path = "test_data.parquet"  # Update with the desired path to save the test data
test_data.write.mode("overwrite").parquet(test_data_path)
print(f"Test data saved to {test_data_path}")

# Stop the SparkSession
spark.stop()

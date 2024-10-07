# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, mean, stddev
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# Step 1: Initialize a Spark session with legacy time parser policy
spark = SparkSession.builder \
    .appName("Stock Prediction") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

# Step 2: Load the dataset
file_path = "CHRW.csv"  # Replace with your file path
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Show the first few rows of the DataFrame
df.show(5)

# Step 3: Data Preparation
# Convert 'Date' to a proper date format
df = df.withColumn("Date", to_date(col("Date"), "dd-MMM-yy"))

# Show the updated DataFrame
df.show(5)

# Step 4: Define Probability Distributions
# Calculate mean and standard deviation for the 'Close' price
stats = df.select(mean("Close").alias("mean_close"), stddev("Close").alias("std_close")).first()
mean_close = stats['mean_close']
std_close = stats['std_close']

# Define a normal distribution using the calculated parameters
def normal_distribution(mean, std, size=1):
    return np.random.normal(mean, std, size)

# Example: Generate a sample from this distribution
sample_size = 10
sample = normal_distribution(mean_close, std_close, sample_size)
print("Sample from Normal Distribution of Close Prices:", sample)

# Step 5: Applying Distributions for Predictions
# Function to simulate future prices
def simulate_future_prices(mean, std, num_days):
    return np.random.normal(mean, std, num_days)

# Simulate prices for the next 10 days
future_prices = simulate_future_prices(mean_close, std_close, 10)
print("Simulated Future Prices:", future_prices)

# Step 6: Analysis and Visualization
# Convert historical close prices to Pandas DataFrame for plotting
historical_prices = df.select("Date", "Close").toPandas()

# Ensure the 'Date' column is in datetime format
historical_prices['Date'] = pd.to_datetime(historical_prices['Date'])

# Create a date range for future prices
future_dates = pd.date_range(start=historical_prices['Date'].iloc[-1] + pd.Timedelta(days=1), periods=10)

# Convert future_prices to a 1D NumPy array
future_prices = np.array(future_prices).flatten()

# Ensure both arrays are of type float (if necessary)
future_prices = future_prices.astype(float)

# Debugging: print shapes and types
print("Shape of future_dates:", future_dates.shape, "Type:", type(future_dates))
print("Shape of future_prices:", future_prices.shape, "Type:", type(future_prices))

# Plotting
plt.figure(figsize=(12, 6))
plt.plot(historical_prices['Date'].values, historical_prices['Close'].values, label='Historical Prices', marker='o')
plt.plot(future_dates.values, future_prices, label='Simulated Future Prices', marker='x')
plt.title('Stock Price Prediction')
plt.xlabel('Date')
plt.ylabel('Stock Price')
plt.legend()
plt.grid()
plt.show()

# Stop the Spark session
spark.stop()

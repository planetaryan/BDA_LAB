from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, FloatType, StringType
import numpy as np
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Monte Carlo Stock Price Simulation") \
    .getOrCreate()

# Load the dataset
data_path = "CHRW.csv"  # Update with your dataset path
schema = StructType([
    StructField("Date", StringType(), True),
    StructField("Open", FloatType(), True),
    StructField("High", FloatType(), True),
    StructField("Low", FloatType(), True),
    StructField("Close", FloatType(), True),
    StructField("Volume", FloatType(), True)
])

# Read CSV file
df = spark.read.csv(data_path, header=True, schema=schema)

# Convert date to proper format if necessary
df = df.withColumn("Date", F.to_date(F.col("Date"), "dd-MMM-yy"))

# Select the necessary column (Closing prices)
close_prices_df = df.select("Close")

# Convert to Pandas DataFrame for easier manipulation
close_prices_pd = close_prices_df.toPandas()

# Calculate daily returns
close_prices_pd['Return'] = close_prices_pd['Close'].pct_change()
returns = close_prices_pd['Return'].dropna().values

# Define parameters for Monte Carlo simulation
num_simulations = 1000
num_days = 30  # Forecasting for the next 30 days
last_price = close_prices_pd['Close'].iloc[-1]

# Run Monte Carlo simulations
simulations = np.zeros((num_simulations, num_days))

for i in range(num_simulations):
    daily_returns = np.random.choice(returns, size=num_days, replace=True)
    price_series = [last_price]
    
    for return_rate in daily_returns:
        price_series.append(price_series[-1] * (1 + return_rate))
    
    simulations[i] = price_series[1:]  # Exclude the initial price

# Convert results to DataFrame for analysis
simulated_prices_df = pd.DataFrame(simulations)

# Analyze the simulation results
mean_prices = simulated_prices_df.mean()
median_prices = simulated_prices_df.median()
percentile_10 = simulated_prices_df.quantile(0.1)
percentile_90 = simulated_prices_df.quantile(0.9)

# Print the results
print("Mean Prices:\n", mean_prices)
print("Median Prices:\n", median_prices)
print("10th Percentile Prices:\n", percentile_10)
print("90th Percentile Prices:\n", percentile_90)

# Stop Spark session
spark.stop()

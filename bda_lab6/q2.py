from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, OneHotEncoder

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Data Preprocessing") \
    .getOrCreate()

# Load the dataset into a DataFrame
file_path = "path/to/your/dataset.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Fill missing values
df_filled = df.fillna({
    'numeric_column': 0,
    'categorical_column': 'Unknown'
})

# Handle categorical features
# Indexing categorical columns
indexers = [
    StringIndexer(inputCol=column, outputCol=column + "_index")
    for column in ['categorical_column1', 'categorical_column2']
]

# Apply indexing
for indexer in indexers:
    df_filled = indexer.fit(df_filled).transform(df_filled)

# One-hot encoding
encoders = [
    OneHotEncoder(inputCols=[column + "_index"], outputCols=[column + "_vec"])
    for column in ['categorical_column1', 'categorical_column2']
]

# Apply encoding
for encoder in encoders:
    df_filled = encoder.fit(df_filled).transform(df_filled)

# Drop original categorical columns
columns_to_drop = ['categorical_column1', 'categorical_column2']
df_final = df_filled.drop(*columns_to_drop)

# Show the final DataFrame
df_final.show()

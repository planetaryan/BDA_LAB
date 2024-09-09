from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Dataset Analysis") \
    .getOrCreate()

# Path to the dataset
file_path = "/home/lplab/Desktop/220962408_PySpark_Week3/stats.csv"

# Load the dataset into a DataFrame
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Show the first few rows of the DataFrame
df.show(5)

# Print the schema of the DataFrame
df.printSchema()

# Display basic statistics for numerical columns
df.describe().show()

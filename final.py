from pyspark.sql import SparkSession
from pyspark.ml.feature import Imputer, VectorAssembler, StandardScaler, StringIndexer, OneHotEncoder
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DoubleType
from pyspark.ml.linalg import Vectors
from pyspark.ml import Pipeline
import numpy as np

# Initialize Spark session
spark = SparkSession.builder.appName("KMeansAnomalyDetection").getOrCreate()

# Load KDD Cup dataset
# Ensure you provide the correct path to your dataset file
data = spark.read.csv("kddcup.data_10_percent_corrected", header=False, inferSchema=True)

# Define schema for the KDD Cup dataset based on its structure
# Assuming the dataset has 41 columns. Adjust this based on actual dataset schema
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

schema = StructType([
    StructField("duration", IntegerType(), True),
    StructField("protocol_type", StringType(), True),
    StructField("service", StringType(), True),
    StructField("flag", StringType(), True),
    StructField("src_bytes", IntegerType(), True),
    StructField("dst_bytes", IntegerType(), True),
    StructField("land", IntegerType(), True),
    StructField("wrong_fragment", IntegerType(), True),
    StructField("urgent", IntegerType(), True),
    # Add the rest of the fields according to the dataset structure
    # Here, for brevity, only a few fields are added
])

data = spark.read.csv("kddcup.data_10_percent_corrected", header=False, schema=schema)

# 1. Handle categorical features
indexers = [
    StringIndexer(inputCol="protocol_type", outputCol="protocol_type_index"),
    StringIndexer(inputCol="service", outputCol="service_index"),
    StringIndexer(inputCol="flag", outputCol="flag_index")
]

# One-hot encode the categorical features
encoders = [
    OneHotEncoder(inputCols=["protocol_type_index"], outputCols=["protocol_type_vec"]),
    OneHotEncoder(inputCols=["service_index"], outputCols=["service_vec"]),
    OneHotEncoder(inputCols=["flag_index"], outputCols=["flag_vec"])
]

# Assemble all features into a vector
feature_cols = ["duration", "protocol_type_vec", "service_vec", "flag_vec", "src_bytes", "dst_bytes", "land", "wrong_fragment", "urgent"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# Scale features
scaler = StandardScaler(inputCol="features", outputCol="scaled_features")

# Create and fit the pipeline
pipeline = Pipeline(stages=indexers + encoders + [assembler, scaler])
pipeline_model = pipeline.fit(data)
processed_data = pipeline_model.transform(data)

# 2. Perform K-means clustering
kmeans = KMeans(k=2, seed=1)
kmeans_model = kmeans.fit(processed_data)
predictions = kmeans_model.transform(processed_data)

# Collect cluster centers as Python objects
centers = [Vectors.dense(center).toArray() for center in kmeans_model.clusterCenters()]

# Define a UDF to calculate the Euclidean distance between two vectors
def calculate_distance(v1, centers):
    v1 = np.array(v1.toArray())  # Convert Spark vector to NumPy array
    distances = [np.sqrt(np.sum(np.square(v1 - center))) for center in centers]
    return float(min(distances))

distance_udf = udf(lambda v: calculate_distance(v, centers), DoubleType())

# Add distance column
results = predictions.withColumn("distance", distance_udf(col("scaled_features")))

# Define a threshold for anomaly detection
threshold = 1000  # This threshold might need adjustment based on the context
results = results.withColumn("is_anomaly", col("distance") > threshold)

# 3. Evaluate the K-means clustering model
evaluator = ClusteringEvaluator()
silhouette = evaluator.evaluate(predictions)
print(f"Silhouette with squared Euclidean distance = {silhouette}")

# Show final results
results.show()

# Stop Spark session
spark.stop()

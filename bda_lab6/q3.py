from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Decision Tree Training") \
    .getOrCreate()

# Load the dataset into a DataFrame
file_path = "path/to/your/dataset.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Fill missing values (if any)
df = df.fillna(0)  # Adjust based on your data

# Define feature columns and label column
feature_columns = ['feature1', 'feature2', 'feature3']  # Replace with actual feature columns
label_column = 'label'  # Replace with the actual label column

# Assemble feature columns into a feature vector
assembler = VectorAssembler(inputCols=feature_columns, outputCol='features')
df = assembler.transform(df)

# Split the dataset into training and testing sets
train_df, test_df = df.randomSplit([0.7, 0.3], seed=1234)

# Initialize the Decision Tree Classifier
dt = DecisionTreeClassifier(featuresCol='features', labelCol=label_column)

# Train the model
dt_model = dt.fit(train_df)

# Make predictions on the test set
predictions = dt_model.transform(test_df)

# Evaluate the model
evaluator = MulticlassClassificationEvaluator(
    labelCol=label_column,
    predictionCol='prediction',
    metricName='accuracy'
)

accuracy = evaluator.evaluate(predictions)
print(f"Test Accuracy = {accuracy:.4f}")

# Stop the Spark session
spark.stop()

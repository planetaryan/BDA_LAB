from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Decision Tree Evaluation") \
    .getOrCreate()

# Load the dataset into a DataFrame
file_path = "path/to/your/dataset.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Fill missing values
df = df.fillna(0)  # Adjust based on your data

# Define feature columns and label column
feature_columns = ['feature1', 'feature2', 'feature3']  # Replace with actual feature columns
label_column = 'label'  # Replace with the actual label column

# Assemble feature columns into a feature vector
assembler = VectorAssembler(inputCols=feature_columns, outputCol='features')
df = assembler.transform(df)

# Split the dataset into training and testing sets
train_df, test_df = df.randomSplit([0.7, 0.3], seed=1234)

# Initialize and train the Decision Tree Classifier
dt = DecisionTreeClassifier(featuresCol='features', labelCol=label_column)
dt_model = dt.fit(train_df)

# Make predictions on the test set
predictions = dt_model.transform(test_df)

# Evaluate the model using accuracy
accuracy_evaluator = MulticlassClassificationEvaluator(
    labelCol=label_column,
    predictionCol='prediction',
    metricName='accuracy'
)
accuracy = accuracy_evaluator.evaluate(predictions)
print(f"Test Accuracy = {accuracy:.4f}")

# Calculate precision and recall
def calculate_precision_recall(predictions, label_col, prediction_col):
    # Create confusion matrix
    confusion_matrix = predictions.groupBy(label_col, prediction_col).count().collect()
    
    # Extract counts
    counts = { (row[label_col], row[prediction_col]): row['count'] for row in confusion_matrix }
    
    # True Positives, False Positives, False Negatives, True Negatives
    TP = counts.get((1.0, 1.0), 0)
    FP = counts.get((0.0, 1.0), 0)
    FN = counts.get((1.0, 0.0), 0)
    TN = counts.get((0.0, 0.0), 0)
    
    # Precision and Recall
    precision = TP / (TP + FP) if (TP + FP) > 0 else 0
    recall = TP / (TP + FN) if (TP + FN) > 0 else 0
    
    return precision, recall

precision, recall = calculate_precision_recall(predictions, label_column, 'prediction')
print(f"Test Precision = {precision:.4f}")
print(f"Test Recall = {recall:.4f}")

# Stop the Spark session
spark.stop()

from pyspark.sql import functions as F
from pyspark.sql.functions import col

def calculate_top_n_recommendations(model, df, k):
    """
    Generate top N recommendations for each user and join with the actual ratings.
    """
    user_recs = model.recommendForAllUsers(k)
    user_recs = user_recs.withColumn("recommendations", F.explode("recommendations"))

    # Flatten the recommendations for evaluation
    recommendations = user_recs.select(col("user"), col("recommendations.item").alias("item"))
    return recommendations

def precision_at_k(predictions, recommendations, k):
    """
    Calculate precision at k.
    """
    # Join the predictions with recommendations
    joined = recommendations.join(predictions, ["user", "item"], "inner")
    relevant_items = joined.groupBy("user").count().withColumnRenamed("count", "relevant_count")

    recommended_items = recommendations.groupBy("user").count().withColumnRenamed("count", "recommended_count")
    joined_stats = relevant_items.join(recommended_items, "user")

    precision = joined_stats.withColumn("precision_at_k", col("relevant_count") / col("recommended_count"))
    return precision.select("precision_at_k").agg(F.avg("precision_at_k")).collect()[0][0]

def recall_at_k(predictions, recommendations, k):
    """
    Calculate recall at k.
    """
    # Join the predictions with recommendations
    joined = recommendations.join(predictions, ["user", "item"], "inner")
    relevant_items = joined.groupBy("user").count().withColumnRenamed("count", "relevant_count")

    recommended_items = predictions.groupBy("user").count().withColumnRenamed("count", "predicted_count")
    joined_stats = relevant_items.join(recommended_items, "user")

    recall = joined_stats.withColumn("recall_at_k", col("relevant_count") / col("predicted_count"))
    return recall.select("recall_at_k").agg(F.avg("recall_at_k")).collect()[0][0]

# Generate top 10 recommendations for each user
recommendations = calculate_top_n_recommendations(model, test_data, k=10)

# Calculate Precision and Recall
precision = precision_at_k(test_data, recommendations, k=10)
recall = recall_at_k(test_data, recommendations, k=10)

print(f"Precision at 10: {precision}")
print(f"Recall at 10: {recall}")

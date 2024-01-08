from kafka import KafkaConsumer, KafkaProducer
import json
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
from pymongo import MongoClient
import pandas as pd

# Connect to Kafka
consumer = KafkaConsumer('product_ratings', bootstrap_servers='localhost:9092', group_id='recommendation_group', value_deserializer=json.loads)
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Create a Spark session
spark = SparkSession.builder.appName("RecommendationConsumer").getOrCreate()

# MongoDB connection
mongo_client = MongoClient('localhost', 27017)
db = mongo_client['recommendations_db']
recommendations_collection = db['recommendations']

# Load historical ratings from a CSV file
historical_ratings_df = pd.read_csv('ratings.csv')

# Create a Spark DataFrame
historical_ratings_spark = spark.createDataFrame(historical_ratings_df, ["user_id", "product_id", "rating"])

# Create StringIndexers for user_id, product_id, and rating
user_id_indexer = StringIndexer(inputCol="user_id", outputCol="user_id_numeric", handleInvalid="keep")
product_id_indexer = StringIndexer(inputCol="product_id", outputCol="product_id_numeric", handleInvalid="keep")
rating_indexer = StringIndexer(inputCol="rating", outputCol="rating_numeric", handleInvalid="keep")

# Create a Pipeline with StringIndexers
pipeline = Pipeline(stages=[user_id_indexer, product_id_indexer, rating_indexer])

# Fit the pipeline to historical ratings
model = pipeline.fit(historical_ratings_spark)
historical_ratings_spark = model.transform(historical_ratings_spark)

# Recommendation system function using PySpark MLlib
def generate_recommendations(user_ratings):
    # Append the newly provided ratings to the historical ratings DataFrame
    user_ratings_df = spark.createDataFrame(user_ratings, ["user_id", "product_id", "rating"])

    # Apply the same pipeline to user ratings
    user_ratings_df = model.transform(user_ratings_df)

    updated_ratings_df = historical_ratings_spark.union(user_ratings_df)

    #updated_ratings_df.show()

    # Build the recommendation model using ALS
    als = ALS(maxIter=5, regParam=0.01, userCol="user_id_numeric", itemCol="product_id_numeric", ratingCol="rating_numeric")
    model_als = als.fit(updated_ratings_df)

    # Get the user_id for whom we just entered a rating
    target_user_id = user_ratings[0][0]

    # Generate recommendations for the target user
    userRecs = model_als.recommendForUserSubset(updated_ratings_df.filter(updated_ratings_df.user_id_numeric == target_user_id), 5)

    # Collect recommendations from the PySpark DataFrame
    collected_recommendations = userRecs.collect()

    print("--Collected recommendations--")
    print(collected_recommendations)

    # Check if there are any recommendations
    if collected_recommendations:
        # Access the first element (assuming there's only one user)
        recommendations = [(int(rec.product_id_numeric), float(rec.rating)) for rec in collected_recommendations[0]['recommendations']]
        
        # Store the recommendation results in MongoDB
        user_id_numeric = int(collected_recommendations[0].user_id_numeric)
        document = {"user_id_numeric": user_id_numeric, "recommendations": recommendations}
        recommendations_collection.insert_one(document)
    else:
        print("No recommendations found.")


# Kafka consumer loop
def consume_messages():
    user_ratings = []
    for message in consumer:
        data = message.value
        user_id = data.get('user_id')
        product_id = data.get('product_id')
        rating = data.get('rating')

        # Append the received rating to the user_ratings list
        user_ratings.append((user_id, product_id, rating))

        if user_id is None and product_id is None and rating is None:
            # Special message indicating the end of data production
            print("Received end-of-production signal. Exiting...")
            break

        print(f"Received rating: User ID={user_id}, Product ID={product_id}, Rating={rating}")

    # Trigger the recommendation system using PySpark MLlib
    generate_recommendations(user_ratings)

    print('Messages consumed and processed')

if __name__ == '__main__':
    print("Console-based Kafka Consumer")
    print("Press Ctrl+C to exit")

    try:
        while True:
            consume_messages()
    except KeyboardInterrupt:
        print("\nExiting...")

    # Stop the Spark session
    spark.stop()

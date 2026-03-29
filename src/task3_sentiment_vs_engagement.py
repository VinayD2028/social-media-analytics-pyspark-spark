"""
task3_sentiment_vs_engagement.py
--------------------------------
Correlates post sentiment polarity with user engagement (likes and retweets).

Approach:
  - Load posts data from CSV with automatic schema inference
  - Classify each post into one of three sentiment categories using SentimentScore:
      Positive  : SentimentScore >  0.3
      Neutral   : SentimentScore between -0.3 and 0.3 (inclusive)
      Negative  : SentimentScore < -0.3
  - Group posts by their sentiment category
  - Compute average likes and average retweets per sentiment group
  - Sort by average likes descending to reveal which sentiment drives the most engagement
  - Write the result to a single CSV output file

Output: outputs/sentiment_engagement.csv
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, avg, col

# Initialize a Spark session for distributed processing
spark = SparkSession.builder.appName("SentimentVsEngagement").getOrCreate()

# Load the social media posts dataset with header row and type inference
posts_df = spark.read.option("header", True).csv("input/posts.csv", inferSchema=True)

# Classify each post into a sentiment bucket based on its pre-computed SentimentScore,
# then aggregate average engagement metrics per sentiment category
sentiment_stats = (
    posts_df
    .withColumn(
        "Sentiment",
        when(col("SentimentScore") > 0.3, "Positive")     # strong positive sentiment
        .when(col("SentimentScore") < -0.3, "Negative")   # strong negative sentiment
        .otherwise("Neutral")                               # neutral or borderline sentiment
    )
    .groupBy("Sentiment")                                   # group by the derived sentiment label
    .agg(
        avg("Likes").alias("AvgLikes"),                     # mean likes per sentiment category
        avg("Retweets").alias("AvgRetweets")                # mean retweets per sentiment category
    )
    .orderBy(col("AvgLikes").desc())                        # surface highest-engagement sentiment first
)

# Coalesce to a single partition before writing to produce one output file
sentiment_stats.coalesce(1).write.mode("overwrite").csv("outputs/sentiment_engagement.csv", header=True)

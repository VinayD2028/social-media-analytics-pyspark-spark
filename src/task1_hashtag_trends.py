"""
task1_hashtag_trends.py
-----------------------
Analyzes trending hashtags across all social media posts.

Approach:
  - Load posts data from CSV
  - Split the comma-delimited Hashtags column into individual tags using explode + split
  - Count the frequency of each unique hashtag across all posts
  - Sort by frequency descending and return the top 10 trending hashtags
  - Write the ranked result to a single CSV output file

Output: outputs/hashtag_trends.csv
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

# Initialize a Spark session for distributed processing
spark = SparkSession.builder.appName("HashtagTrends").getOrCreate()

# Load the social media posts dataset with header inference
posts_df = spark.read.option("header", True).csv("input/posts.csv")

# Explode the comma-separated Hashtags field into one row per hashtag,
# then group by hashtag and count occurrences, sorted by frequency descending
hashtag_counts = (
    posts_df
    .select(explode(split(col("Hashtags"), ",")).alias("Hashtag"))  # flatten hashtag arrays
    .groupBy("Hashtag")                                              # group by each unique hashtag
    .count()                                                         # count how many times each appears
    .orderBy(col("count").desc())                                    # sort by most frequent first
    .limit(10)                                                       # keep only top 10
)

# Coalesce to a single partition before writing to produce one output file
hashtag_counts.coalesce(1).write.mode("overwrite").csv("outputs/hashtag_trends.csv", header=True)

"""
task4_top_verified_users.py
---------------------------
Identifies the top 5 most influential verified users by total post reach.

Approach:
  - Load both posts and users datasets from CSV with schema inference
  - Filter the users dataset to retain only verified accounts (Verified == True)
  - Join filtered users with posts on UserID to get only posts by verified users
  - Compute per-post reach as the sum of Likes and Retweets
  - Aggregate total reach per user across all their posts using sum
  - Sort by total reach descending to rank the most influential influencers
  - Limit results to the top 5 users
  - Write the leaderboard to a single CSV output file

Output: outputs/top_verified_users.csv
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

# Initialize a Spark session for distributed processing
spark = SparkSession.builder.appName("TopVerifiedUsers").getOrCreate()

# Load the posts dataset with header row and automatic type inference
posts_df = spark.read.option("header", True).csv("input/posts.csv", inferSchema=True)

# Load the users dataset with header row and automatic type inference
users_df = spark.read.option("header", True).csv("input/users.csv", inferSchema=True)

# Filter to verified users only, join with posts to get their content,
# calculate per-post reach, aggregate by user, and return the top 5
top_verified = (
    posts_df.join(users_df.filter(col("Verified") == True), "UserID")  # only posts by verified users
    .withColumn("TotalReach", col("Likes") + col("Retweets"))           # reach = likes + retweets per post
    .groupBy("Username")                                                  # aggregate across all posts per user
    .agg(_sum("TotalReach").alias("TotalReach"))                          # sum reach across all user posts
    .orderBy(col("TotalReach").desc())                                    # highest cumulative reach first
    .limit(5)                                                             # top 5 influencers only
)

# Coalesce to a single partition before writing to produce one output file
top_verified.coalesce(1).write.mode("overwrite").csv("outputs/top_verified_users.csv", header=True)

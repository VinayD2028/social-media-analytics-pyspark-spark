"""
task2_engagement_by_age.py
--------------------------
Analyzes user engagement patterns segmented by demographic age group.

Approach:
  - Load both posts and users datasets from CSV with schema inference
  - Join posts with user profiles on the shared UserID key (inner join)
  - Group the combined dataset by AgeGroup (Teen, Adult, Senior)
  - Compute average likes and average retweets for each group
  - Sort by average likes descending to surface the most engaged demographic
  - Write the aggregated result to a single CSV output file

Output: outputs/engagement_by_age.csv
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

# Initialize a Spark session for distributed processing
spark = SparkSession.builder.appName("EngagementByAgeGroup").getOrCreate()

# Load posts dataset with header row and automatic type inference
posts_df = spark.read.option("header", True).csv("input/posts.csv", inferSchema=True)

# Load users dataset with header row and automatic type inference
users_df = spark.read.option("header", True).csv("input/users.csv", inferSchema=True)

# Join posts with user profiles on UserID to enrich posts with demographic info,
# then group by age segment and aggregate average engagement metrics
engagement_df = (
    posts_df.join(users_df, "UserID")                                         # inner join on UserID
    .groupBy("AgeGroup")                                                       # segment by age group
    .agg(
        avg("Likes").alias("AvgLikes"),                                        # mean likes per age group
        avg("Retweets").alias("AvgRetweets")                                   # mean retweets per age group
    )
    .orderBy(col("AvgLikes").desc())                                           # highest engagement first
)

# Coalesce to a single partition before writing to produce one output file
engagement_df.coalesce(1).write.mode("overwrite").csv("outputs/engagement_by_age.csv", header=True)

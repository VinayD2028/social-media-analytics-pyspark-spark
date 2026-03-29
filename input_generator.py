"""
input_generator.py
------------------
Synthetic dataset generator for the social media analytics pipeline.

This script programmatically creates two realistic CSV datasets:
  - users.csv  : 50 user profiles with demographic and verification metadata
  - posts.csv  : 100 social media posts with engagement metrics and sentiment scores

The generated data simulates a real-world social media platform and is used
as input for the PySpark analytics modules in the src/ directory.

Usage:
    python input_generator.py

Output:
    input/users.csv   - User profiles (UserID, Username, AgeGroup, Country, Verified)
    input/posts.csv   - Post data (PostID, UserID, Content, Timestamp, Likes,
                        Retweets, Hashtags, SentimentScore)
"""

import csv
import os
import random
from datetime import datetime, timedelta

# Create the input directory if it doesn't already exist
os.makedirs("input", exist_ok=True)

# ── User Data Generation ────────────────────────────────────────────────────

user_data = []

# Pool of base usernames; each user gets a unique suffix appended (e.g., @techie421)
usernames = [
    "@techie42", "@critic99", "@daily_vibes", "@designer_dan", "@rage_user",
    "@meme_lord", "@social_queen", "@calm_mind", "@pixel_pusher", "@stream_bot"
]

# Demographic age categories for segmentation analysis
age_groups = ["Teen", "Adult", "Senior"]

# Countries of residence for geographic diversity
countries = ["US", "UK", "Canada", "India", "Germany", "Brazil"]

# Verification status — roughly 50% of users are verified
verified_status = [True, False]

# Generate 50 unique user profiles
for user_id in range(1, 51):
    user = {
        "UserID": user_id,
        "Username": random.choice(usernames) + str(user_id),  # unique handle per user
        "AgeGroup": random.choice(age_groups),                 # random demographic segment
        "Country": random.choice(countries),                   # random country of origin
        "Verified": random.choice(verified_status)             # randomly verified or not
    }
    user_data.append(user)

# Write the user profiles to CSV
with open("input/users.csv", mode="w", newline="") as file:
    writer = csv.DictWriter(file, fieldnames=user_data[0].keys())
    writer.writeheader()
    writer.writerows(user_data)

# ── Post Data Generation ────────────────────────────────────────────────────

# Pool of hashtags to be randomly assigned to posts (1-3 per post)
hashtags_pool = ["#tech", "#fail", "#design", "#UX", "#cleanUI", "#mood", "#bug", "#love", "#social", "#AI"]

# Sample post content covering a range of sentiments (positive, neutral, negative)
contents = [
    "Loving the new update!",
    "This app keeps crashing. So annoying.",
    "Just another day...",
    "Absolutely love the UX!",
    "Worst experience ever.",
    "Such a smooth interface!",
    "Great performance on mobile.",
    "Can't stop using it!",
    "Needs dark mode ASAP!",
    "I'm impressed with the speed."
]

posts_data = []
base_time = datetime.now()  # anchor point for generating timestamps

# Generate 100 posts (PostIDs 101–200) with randomized engagement and sentiment data
for post_id in range(101, 201):
    uid = random.randint(1, 10)  # assign post to one of the first 10 users

    # Generate a timestamp within the past 10 days
    timestamp = (base_time - timedelta(hours=random.randint(0, 240))).strftime("%Y-%m-%d %H:%M:%S")

    content = random.choice(contents)            # random post text
    likes = random.randint(0, 150)               # random like count (0–150)
    retweets = random.randint(0, 50)             # random retweet count (0–50)
    sentiment = round(random.uniform(-1, 1), 2)  # sentiment score in [-1.0, 1.0]
    hashtags = ",".join(random.sample(hashtags_pool, random.randint(1, 3)))  # 1–3 random hashtags

    post = {
        "PostID": post_id,
        "UserID": uid,
        "Content": content,
        "Timestamp": timestamp,
        "Likes": likes,
        "Retweets": retweets,
        "Hashtags": hashtags,
        "SentimentScore": sentiment
    }
    posts_data.append(post)

# Write the posts to CSV with UTF-8 encoding to support special characters
with open("input/posts.csv", mode="w", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(file, fieldnames=posts_data[0].keys())
    writer.writeheader()
    writer.writerows(posts_data)

print("Dataset generation complete: 'users.csv' and 'posts.csv' created in /input/")

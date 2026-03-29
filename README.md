# 📊 Social Media Analytics Pipeline — PySpark & SparkSQL

![Python](https://img.shields.io/badge/Python-3.8%2B-blue?logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-3.x-orange?logo=apache-spark&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-Distributed-red?logo=apache-spark)
![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED?logo=docker&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green)

> A production-style, distributed data analytics pipeline that processes large-scale social media datasets using **Apache Spark Structured APIs (PySpark & SparkSQL)**. This project demonstrates core Data Science and Data Engineering competencies — from ingestion and transformation to insight generation — all running on a distributed compute engine with optional Docker deployment.

---

## 🚀 Project Overview

This pipeline ingests synthetic social media data (posts and user profiles) and runs four independent analytical modules to surface actionable insights:

| Module | Analysis | Output |
|--------|----------|--------|
| **Hashtag Trends** | Identifies the top 10 trending hashtags by frequency | `outputs/hashtag_trends.csv` |
| **Engagement by Age Group** | Computes average likes & retweets across demographic segments | `outputs/engagement_by_age.csv` |
| **Sentiment vs Engagement** | Correlates post sentiment (positive/neutral/negative) with engagement | `outputs/sentiment_engagement.csv` |
| **Top Verified Influencers** | Ranks verified users by total post reach (likes + retweets) | `outputs/top_verified_users.csv` |

---

## 🛠️ Tech Stack & Skills Demonstrated

| Category | Tools / Concepts |
|----------|-----------------|
| **Distributed Computing** | Apache Spark 3.x, PySpark, SparkSQL |
| **Data Transformation** | Spark DataFrames, SparkSQL Structured APIs, `explode`, `split`, `when`, window logic |
| **Data Engineering** | ETL pipeline design, schema inference, CSV I/O, coalesce for single-file output |
| **Sentiment Analysis** | Rule-based NLP classification using sentiment score thresholds |
| **Statistical Analysis** | Aggregations (`avg`, `sum`), groupBy, ranking, filtering |
| **Containerization** | Docker & Docker Compose for portable Spark cluster deployment |
| **Data Generation** | Synthetic dataset generation with Python (`csv`, `random`, `datetime`) |
| **Languages** | Python 3.x |

---

## 📁 Project Structure

```
social-media-analytics-pyspark-spark/
├── input/
│   ├── posts.csv              # Social media posts dataset (100+ records)
│   └── users.csv              # User profiles dataset (50 users)
├── outputs/
│   ├── hashtag_trends.csv     # Top 10 trending hashtags
│   ├── engagement_by_age.csv  # Avg engagement by age group
│   ├── sentiment_engagement.csv  # Engagement by sentiment category
│   └── top_verified_users.csv    # Top 5 verified influencers by reach
├── src/
│   ├── task1_hashtag_trends.py         # Hashtag frequency analysis
│   ├── task2_engagement_by_age.py      # Age-group engagement analysis
│   ├── task3_sentiment_vs_engagement.py  # Sentiment-engagement correlation
│   └── task4_top_verified_users.py     # Influencer reach ranking
├── docker-compose.yml         # Multi-node Spark cluster setup
├── input_generator.py         # Synthetic dataset generator
└── README.md
```

---

## 📊 Dataset Schema

### `posts.csv`
| Column | Type | Description |
|--------|------|-------------|
| PostID | Integer | Unique post identifier |
| UserID | Integer | Foreign key to users.csv |
| Content | String | Text content of the post |
| Timestamp | String | Post datetime (YYYY-MM-DD HH:MM:SS) |
| Likes | Integer | Number of likes received |
| Retweets | Integer | Number of retweets/shares |
| Hashtags | String | Comma-separated hashtags (e.g., `#AI,#tech`) |
| SentimentScore | Float | Pre-scored sentiment in range [-1.0, 1.0] |

### `users.csv`
| Column | Type | Description |
|--------|------|-------------|
| UserID | Integer | Unique user identifier |
| Username | String | User handle (e.g., `@techie42`) |
| AgeGroup | String | Demographic category: Teen / Adult / Senior |
| Country | String | Country of residence |
| Verified | Boolean | Whether the account is verified |

---

## 🔍 Analysis Modules

### 1. Hashtag Trends
Splits the comma-delimited `Hashtags` column, explodes it into individual rows, counts occurrences per hashtag, and returns the **top 10 most-used hashtags** sorted by frequency descending.

**Key Spark operations:** `explode`, `split`, `groupBy`, `count`, `orderBy`, `limit`

### 2. Engagement by Age Group
Joins `posts.csv` and `users.csv` on `UserID`, then aggregates average likes and retweets per demographic age group. Results are sorted to surface the most-engaged segments.

**Key Spark operations:** `join`, `groupBy`, `agg(avg)`, `orderBy`

### 3. Sentiment vs Engagement
Classifies each post into **Positive** (score > 0.3), **Neutral** (-0.3 to 0.3), or **Negative** (score < -0.3) sentiment buckets using conditional logic, then measures average engagement per category.

**Key Spark operations:** `withColumn`, `when`/`otherwise`, `groupBy`, `agg(avg)`

### 4. Top Verified Influencers by Reach
Filters for verified accounts only, computes total reach per user (Likes + Retweets), aggregates across all posts, and returns the **top 5 influencers** ranked by cumulative reach.

**Key Spark operations:** `filter`, `join`, `withColumn`, `groupBy`, `agg(sum)`, `orderBy`, `limit`

---

## ⚙️ Setup & Execution

### Prerequisites
- Python 3.8+
- PySpark: `pip install pyspark`
- Apache Spark 3.x ([Download](https://spark.apache.org/downloads.html))
- Docker & Docker Compose *(optional)*

### Step 1: Generate the Dataset
```bash
python input_generator.py
```
This creates `input/posts.csv` and `input/users.csv` with realistic synthetic data.

### Step 2: Run Analysis Modules (Local)
```bash
spark-submit src/task1_hashtag_trends.py
spark-submit src/task2_engagement_by_age.py
spark-submit src/task3_sentiment_vs_engagement.py
spark-submit src/task4_top_verified_users.py
```
Results are saved to the `outputs/` directory.

### Step 3: Run with Docker (Optional — Distributed Cluster)
```bash
# Start a multi-node Spark cluster
docker-compose up -d

# Shell into the Spark master node
docker exec -it spark-master bash

# Submit jobs from inside the container
spark-submit src/task1_hashtag_trends.py
spark-submit src/task2_engagement_by_age.py
spark-submit src/task3_sentiment_vs_engagement.py
spark-submit src/task4_top_verified_users.py

# Verify outputs
ls outputs/

# Shut down the cluster
exit
docker-compose down
```

---

## 📈 Sample Results

### Top Hashtags
| Hashtag | Count |
|---------|-------|
| #AI | 38 |
| #tech | 35 |
| #cleanUI | 32 |

### Engagement by Age Group
| Age Group | Avg Likes | Avg Retweets |
|-----------|-----------|--------------|
| Senior | 82.1 | 21.3 |
| Adult | 75.5 | 21.3 |
| Teen | 70.5 | 21.3 |

### Sentiment vs Engagement
| Sentiment | Avg Likes | Avg Retweets |
|-----------|-----------|--------------|
| Positive | 82.6 | 25.5 |
| Neutral | 81.3 | 22.6 |
| Negative | 65.2 | 20.7 |

---

## 💡 Key Insights

- **Positive sentiment posts drive significantly higher engagement** — positive posts average ~27% more likes than negative ones, highlighting the importance of tone in content strategy.
- **Senior users show the highest average engagement**, suggesting they interact more deeply with content compared to younger demographics.
- **#AI and #tech are the dominant trending hashtags**, reflecting strong user interest in technology topics on the platform.
- **Verified influencers generate disproportionate reach**, making them high-value targets for platform monetization and partnership strategies.

---

## 🏗️ Architecture

```
+---------------------+
|  input_generator.py |  <- Synthetic data generation (Python)
+----------+----------+
           |
           v
+---------------------+
|   input/            |  <- posts.csv + users.csv
+----------+----------+
           |
           v
+----------------------------------------------+
|        Apache Spark Cluster                  |
|  (Local Mode or Docker Multi-Node)           |
|                                              |
|  +----------------+  +------------------+   |
|  |  task1         |  |  task2           |   |
|  |  Hashtag       |  |  Engagement      |   |
|  |  Trends        |  |  by Age Group    |   |
|  +----------------+  +------------------+   |
|  +----------------+  +------------------+   |
|  |  task3         |  |  task4           |   |
|  |  Sentiment     |  |  Top Verified    |   |
|  |  vs Engagement |  |  Influencers     |   |
|  +----------------+  +------------------+   |
+------------------+---------------------------+
                   |
                   v
+---------------------+
|   outputs/          |  <- CSV results for each module
+---------------------+
```

---

## 📄 License

This project is open-source and available under the [MIT License](LICENSE).

---

*Built with Apache Spark, PySpark, and Python*

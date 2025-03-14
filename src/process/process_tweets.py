from kafka import KafkaConsumer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import json
import time
import logging
import pandas as pd
import io
import boto3
from botocore.exceptions import ClientError
import psycopg2

# Setting up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers= [
        logging.FileHandler("tweet_process_log.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Setting up Kafka Consumer
consumer = KafkaConsumer("tweets", bootstrap_servers="localhost:9092", value_deserializer=lambda x: json.loads(x.decode("utf-8")), auto_offset_reset="latest")

# Set up S3 Client
s3_client = boto3.client("s3")

# Setting up Vader
analyzer = SentimentIntensityAnalyzer()

# S3 bucket details
PROCESSED_BUCKET = "deepaanna-twitter-processed"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "tweets_db"
DB_USER = "postgres"
DB_PASSWORD = "Newman1124!"

conn = psycopg2.connect(
    host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
)
cursor = conn.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS tweets (
    id VARCHAR(36) PRIMARY KEY,
    text TEXT,
    sentiment VARCHAR(10),
    created_at TIMESTAMP
    )
""")
conn.commit()

def save_processed_to_s3(bucket, key, data_list):
    try:
        # Convert to DataFrame and save as Parquet
        df = pd.DataFrame(data_list)
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine="pyarrow")
        s3_client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    except ClientError as e:
        logger.error(f"Failed to save to S3: {e}")

def save_to_database(data_list):
    try:
        for tweet in data_list:
            cursor.execute("""
                INSERT INTO tweets (id, text, sentiment, created_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (tweet["id"], tweet["text"], tweet["sentiment"], tweet["created_at"]))
        conn.commit()
    except Exception as e:
        logger.error(f"Failed to save to database: {e}")
        conn.rollback()

logger.info("Starting tweet processing...")
tweet_count = 0
start_time = time.time()
CHECK_INTERVAL = 300

processed_tweets = [] # Batching for S3

try:
    for message in consumer:
        tweet = message.value
        text = tweet["text"]
        sentiment_score = analyzer.polarity_scores(text)
        sentiment = "postive" if sentiment_score["compound"] > 0.05 else "negative" if sentiment_score["compound"] < -0.05 else "neutral"

        tweet_count += 1
        logger.info(f"Processed: {tweet['text']} -> {sentiment}")

        # Add to batch
        processed_tweet = { 
            "id": tweet['id'],
            "text": text,
            "sentiment": sentiment,
            "created_at": tweet['created_at']
        }
        processed_tweets.append(processed_tweet)
        
        if len(processed_tweets) >= 100:
            s3_key = f"processed/tweets/{int(time.time())}.parquet"
            save_processed_to_s3(PROCESSED_BUCKET, s3_key, processed_tweets)
            save_to_database(processed_tweets)
            logger.info(f"Saved {len(processed_tweets)} processed tweets to S3 and database")
            processed_tweets.clear()
            processed_tweets = []

        elapsed_time = time.time() - start_time
        if elapsed_time >= CHECK_INTERVAL:
            tweets_per_minute = tweet_count / (elapsed_time / 60)
            logger.info(f"Processed {tweet_count} tweets in {elapsed_time:.1f} seconds (~{tweets_per_minute:.1f} tweets/minute)")

            tweet_count = 0
            start_time = time.time()

        print(f"Processed: {tweet['text']} -> {sentiment}")
except KeyboardInterrupt:
    # save any remaining tweets
    if len(processed_tweets) > 0:
        s3_key = f"processed/tweets/{int(time.time())}.parquet"
        save_processed_to_s3(PROCESSED_BUCKET, s3_key, processed_tweets)
        save_to_database(processed_tweets)

    elapsed_time = time.time() - start_time
    tweets_per_minute = tweet_count / ( elapsed_time / 60)
    logger.info(f"Stopped processing. Processed {tweet_count} tweets in {elapsed_time:.1f} seconds (~{tweets_per_minute:.1f} tweets/minute)")
finally:
    conn.close()
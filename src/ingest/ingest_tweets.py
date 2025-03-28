 
import json
import time
import random
from kafka import KafkaProducer
from faker import Faker
import logging
import boto3
from botocore.exceptions import ClientError

# Setting up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers= [
        logging.FileHandler("tweet_log.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Initialize Faker for data
fake = Faker()

# Initialize S3 client for storage
s3_client = boto3.client("s3")

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

RAW_BUCKET = "deepaanna-twitter-raw"

crypto_terms = [
    "Bitcoin BTC #BTC",
    "Ethereum ETH #ETH",
    "crypto #crypto",
    "XRP #XRP",
    "Cardano ADA #ADA",
    "Dogecoin #DOGE",
    "Solana SOL #SOL"
]

sentiments = [
    "is pumping!", "just crashed :(", "to the moon!", "Buy now!",
    "HODL!", "selling all my", "best investment ever"
]

def generate_mock_tweet():
    crypto_term = random.choice(crypto_terms)
    sentiment = random.choice(sentiments)
    tweet_text = f"{fake.sentence()} {crypto_term} {sentiment}"
    tweet_data = {
        "id": fake.uuid4(),
        "text": tweet_text,
        "created_at": fake.date_time_this_year().isoformat() + "Z"
    }
    return tweet_data

def save_to_s3(bucket, key, data):
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(data)
        )
    except ClientError as e:
        logger.error(f"Failed to save to S3: {e}")

print("Starting mock tweet stream...")
tweet_count = 0
start_time = time.time()
CHECK_INTERVAL = 300

try:
    while True:
        tweet = generate_mock_tweet()
        producer.send("tweets", tweet)
        tweet_count += 1
        logger.info(f"Sent mock tweet: {tweet['text']}")

        # Save raw tweets to S3
        s3_key = f"raw/tweets/{tweet['created_at']} / {tweet['id']}.json"
        save_to_s3(RAW_BUCKET, s3_key, tweet)

        elapsed_time = time.time() - start_time
        if elapsed_time >= CHECK_INTERVAL:
            tweets_per_minute = tweet_count / (elapsed_time / 60)
            logger.info(f"Sent {tweet_count} tweets in {elapsed_time:.1f} seconds( ~{tweets_per_minute:.1f} tweets/minute)")

            #Reset counters
            tweet_count = 0
            start_time = time.time()

        time.sleep(0.01) # to achieve 10K + tweets
except KeyboardInterrupt:
    elapsed_time = time.time() - start_time
    tweets_per_minute = tweet_count / (elapsed_time / 60) if elapsed_time > 0 else 0

    logger.info(f"Stopped mock tweet stream. Sent {tweet_count} tweets in {elapsed_time:.1f} seconds (~{tweets_per_minute:.1f} tweets/minute)")
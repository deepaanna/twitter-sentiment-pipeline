from kafka import KafkaConsumer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import json
import time
import logging

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

consumer = KafkaConsumer("tweets", bootstrap_servers="localhost:9092", value_deserializer=lambda x: json.loads(x.decode("utf-8")), auto_offset_reset="latest")
analyzer = SentimentIntensityAnalyzer()

logger.info("Starting tweet processing...")
tweet_count = 0
start_time = time.time()
CHECK_INTERVAL = 300

try:
    for message in consumer:
        tweet = message.value
        text = tweet["text"]
        sentiment_score = analyzer.polarity_scores(text)
        sentiment = "postive" if sentiment_score["compound"] > 0.05 else "negative" if sentiment_score["compound"] < -0.05 else "neutral"

        tweet_count += 1
        elapsed_time = time.time() - start_time

        if elapsed_time >= CHECK_INTERVAL:
            tweets_per_minute = tweet_count / (elapsed_time / 60)
            logger.info(f"Processed {tweet_count} tweets in {elapsed_time:.1f} seconds (~{tweets_per_minute:.1f} tweets/minute)")

            tweet_count = 0
            start_time = time.time()

        print(f"Processed: {tweet['text']} -> {sentiment}")
except KeyboardInterrupt:
    elapsed_time = time.time() - start_time
    tweets_per_minute = tweet_count / ( elapsed_time / 60)
    logger.info(f"Stopped processing. Processed {tweet_count} tweets in {elapsed_time:.1f} seconds (~{tweets_per_minute:.1f} tweets/minute)")
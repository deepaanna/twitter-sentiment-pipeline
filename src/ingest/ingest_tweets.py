 
import tweepy
from kafka import KafkaProducer
import json
from dotenv import load_dotenv
import os


load_dotenv("D:/PROJECTS/Data-Engineering-Portfolio/twitter-sentiment-pipeline/config/.env")

BEARER_TOKEN = os.getenv("BEARER_TOKEN")

producer = KafkaProducer(bootstrap_servers="localhost:9092",
                         value_serializer = lambda v: json.dumps(v).encode("utf-8"))

class TweetStreamer(tweepy.StreamingClient):
    def on_tweet(self, tweet):
        tweet_data = {"id": tweet.id,
                      "text": tweet.text,
                      "created_at": str(tweet.created_at)}
        producer.send("tweets", tweet_data)
        print(f"Sent tweet: {tweet.text}")


streamer = TweetStreamer(BEARER_TOKEN)


existing_rules = streamer.get_rules()
if existing_rules.data:
    rule_ids = [rule.id for rule in existing_rules.data]
    streamer.delete_rules(rule_ids)

rule = tweepy.StreamRule(value="AI", tag="ai_tweets")
streamer.add_rules(rule)
print(f"Added rule: {rule}")

streamer.filter()
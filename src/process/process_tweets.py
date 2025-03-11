from kafka import KafkaConsumer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import json

consumer = KafkaConsumer("tweets", bootstrap_servers="localhost:9092", value_deserializer=lambda x: json.loads(x.decode("utf-8")), auto_offset_reset="latest")
analyzer = SentimentIntensityAnalyzer()

for message in consumer:
    tweet = message.value
    text = tweet["text"]
    sentiment_score = analyzer.polarity_scores(text)
    sentiment = "postive" if sentiment_score["compound"] > 0.05 else "negative" if sentiment_score["compound"] < -0.05 else "neutral"

    print(f"Processed: {tweet['text']} -> {sentiment}")

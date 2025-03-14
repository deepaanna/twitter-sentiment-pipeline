import streamlit as st
import pandas as pd
import boto3
import psycopg2
import io
import time
from datetime import datetime
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import os

st.set_page_config(
    page_title="Crypto Tweet Dashboard",
    layout="wide"
)

# S3 config from environment variables
S3_BUCKET= os.getenv("S3_BUCKET", "deepaanna-twitter-processed")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

s3_client = boto3.client("s3",
                         aws_access_key_id=AWS_ACCESS_KEY_ID,
                         aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

@st.cache_data(ttl=10)
def fetch_from_s3():
    try:
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix="processed/tweets/")
        if "Contents" not in response:
            return pd.DataFrame()
        
        latest_file = max(response["Contents"],
                        key = lambda x: x["LastModified"])["Key"]
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=latest_file)
        buffer = io.BytesIO(obj["Body"].read())
        df = pd.read_parquet(buffer)
        return df
    except Exception as e:
        st.error(f"S3 fetch error: {e}")
        return pd.DataFrame()

# Main Dashboard
st.title("Real-Time Crypto Tweet Sentiment Dashboard")

# Side bar
refresh_interval = st.sidebar.slider("Refresh Interval (seconds)", 5, 60, 10)
auto_refresh = st.sidebar.checkbox("Enable Auto-Refresh", value=True)

# Placeholder for dynamic content
placeholder = st.empty()

# real-time update loop
def update_dashboard():
    with placeholder.container():
        df = fetch_from_s3()

        if df.empty:
            st.warning("No data available yet. Run the pipeline to generate tweets!")
        else:
            # Convert created_at to datetime
            df["created_at"] = pd.to_datetime(df["created_at"])

            # Metrics
            total_tweets = len(df)
            st.metric("Total Tweets Processed", total_tweets)

            # Sentiment Distribution ( Bar Chart )
            sentiment_counts = df["sentiment"].value_counts()
            st.subheader("Sentiment Distribution")
            st.bar_chart(sentiment_counts)

            # Sentiment Pie Chart
            st.subheader("Sentiment Distribution ( Pie Chart )")
            fig, ax = plt.subplots()
            ax.pie(sentiment_counts, labels=sentiment_counts.index,
                   autopct="%1.1f%%", startangle=90, colors=["#00FF00", "#FFFF00", "#FF0000"],
                   explode=[0.1, 0, 0])
            ax.axis("equal")
            st.pyplot(fig)

            # time Series of Sentiment
            st.subheader("Sentiment Over Time")
            sentiment_over_time = df.groupby([pd.Grouper(key="created_at", freq="1H"), "sentiment"]).size().unstack(fill_value=0)
            st.line_chart(sentiment_over_time)

            # Word Cloud
            st.subheader("Word Cloud of Tweet Content")
            text = " ".join(df["text"].tolist())
            wordcloud = WordCloud(width=800, height=400, background_color="white").generate(text)
            plt.figure(figsize=(10,5))
            plt.imshow(wordcloud, interpolation="bilinear")
            plt.axis("off")
            st.pyplot(plt)

            # Sentiment Heatmap
            st.subheader("Sentiment Heatmap by Hour")
            df["hour"] = df["created_at"].dt.hour
            heatmap_data = df.pivot_table(
                index="hour", 
                columns="sentiment", 
                values="id",
                aggfunc="count",
                fill_value=0
            )
            st.area_chart(heatmap_data)

            # Top Crypto Mentions
            st.subheader("Top Crypto Mentions")
            crypto_terms= ["Bitcoin", "BTC", "Ethereum", "ETC", "XRP", "Cardano",
                           "ADA", "Dogecoin", "DOGE", "Solana", "SOL", "crypto"]
            crypto_counts = {term: df["text"].str.contains(term, case=False).sum() for term in crypto_terms}
            crypto_df = pd.Series(crypto_counts).sort_values(ascending=False)
            st.bar_chart(crypto_df)

            # Sample Tweets
            st.subheader("Sample Tweets")
            st.dataframe(df[["text", "sentiment", "created_at"]].head(10))
        # Last update
        st.sidebar.write(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

update_dashboard()

if auto_refresh:
    while True:
        time.sleep(refresh_interval)
        update_dashboard()
        st.experimental_rerun()
else:
    if st.button("Manual Refresh"):
        update_dashboard()
        st.experimental_rerun()

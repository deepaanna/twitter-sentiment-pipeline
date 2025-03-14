import streamlit as st
import pandas as pd
import boto3
import psycopg2
import io
import time
from datetime import datetime

st.set_page_config(
    page_title="Crypto Tweet Dashboard",
    layout="wide"
)

S3_BUCKET="deepaanna-twitter-processed"
DB_HOST="localhost"
DB_NAME="tweets_db"
DB_PORT="5432"
DB_USER="postgres"
DB_PASSWORD="Newman1124!"

s3_client = boto3.client("s3")

@st.cache_data(ttl=10)
def fetch_from_postgres():
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD)
    query = "SELECT id, text, sentiment, created_at FROM tweets"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

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
data_source = st.sidebar.selectbox("Data Source", ["PostgreSQL", "S3"])
refresh_interval = st.sidebar.slider("Refresh Interval (seconds)", 5, 60, 10)
auto_refresh = st.sidebar.checkbox("Enable Auto-Refresh", value=True)

# Placeholder for dynamic content
placeholder = st.empty()

# real-time update loop
def update_dashboard():
    with placeholder.container():
        if data_source == "PostgreSQL":
            df = fetch_from_postgres()
        else:
            df = fetch_from_s3()

        if df.empty:
            st.warning("No data available yet. Run the pipeline to generate tweets!")
        else:
            total_tweets = len(df)
            st.metric("Total Tweets Processed", total_tweets)

            sentiment_counts = df["sentiment"].value_counts()
            st.subheader("Sentiment Distribution")
            st.bar_chart(sentiment_counts)

            df["created_at"] = pd.to_datetime(df["created_at"])
            sentiment_over_time = df.groupby([pd.Grouper(key="created_at", freq="1H"), "sentiment"]).size().unstack()
            st.line_chart(sentiment_over_time)


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

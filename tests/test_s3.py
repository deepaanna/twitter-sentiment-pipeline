import boto3
import pandas as pd
import io

s3_client = boto3.client("s3")
bucket = "deepaanna-twitter-processed"
key = "test/test.parquet"
data = [{"id": "1", "text": "Test tweet", "sentiment": "positive", "created_at": "2025-03-12T10:00:00Z"}]
df = pd.DataFrame(data)
buffer = io.BytesIO()
df.to_parquet(buffer, engine="pyarrow")
s3_client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
print("Uploaded test.parquet to S3")
import boto3
import io
import logging
import pandas as pd
import json


def save_to_s3(df, bucket_name, key):
    s3 = boto3.client('s3')
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=bucket_name, Key=key, Body=csv_buffer.getvalue())
    logging.info(f"Saved {key} to S3 bucket {bucket_name}")


def read_from_s3(bucket_name, key):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket_name, Key=key)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    return df

def save_json_to_s3(data, bucket_name, key):
    """Save a Python dict/list as JSON to S3."""
    json_str = json.dumps(data, ensure_ascii=False, indent=2)

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json_str,
        ContentType='application/json'
    )


def read_json_from_s3(bucket_name, key):
    """Read a JSON file from S3 and return a Python object."""
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket_name, Key=key)
    json_str = obj['Body'].read().decode('utf-8')
    return json.loads(json_str)



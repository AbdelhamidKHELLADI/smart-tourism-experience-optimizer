import boto3
import io
import pandas as pd
import json


def read_from_s3(bucket_name, key):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket_name, Key=key)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    return df

def read_json_from_s3(bucket_name, key):
    """Read a JSON file from S3 and return a Python object."""
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket_name, Key=key)
    json_str = obj['Body'].read().decode('utf-8')
    return json.loads(json_str)



import time
import boto3
import os

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET = "naya-finalproject-sources"
date_str = time.strftime("%Y%m%d")

shufersal_catIDs = {"All": "0", "Prices": "1", "PriceFull": "2", "Promos": "3", "PromoFull": "4", "Stores": "5"}

def get_s3():
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

def get_access_key() -> str:
    return AWS_ACCESS_KEY_ID

def get_secret_key() -> str:
    return AWS_SECRET_ACCESS_KEY

def get_bucket_name() -> str:
    return S3_BUCKET

def get_date_prefix() -> str:
    return time.strftime("%Y-%m-%d/")

def get_chains(s3) -> dict:
    
    bucket = "naya-finalproject-sources"
    key = "chains/Chains.csv"
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read().decode("utf-8", errors="replace")
        lines = list(map(lambda line: line.split(","), body.splitlines()))
    except Exception as e:
        raise RuntimeError(f"Failed to load Chains.csv from S3 ({bucket}/{key}): {e}")
    # Build dict: first column -> last column, skip header
    out = {line[0]: line[-1] for line in lines[1:]}
    return out

def get_chain_name(store_id: str, chains_dict: dict) -> str:
    """Return chain name for given store ID using provided chains dictionary."""
    return chains_dict.get(store_id, "Unknown")

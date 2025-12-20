from asyncio.log import logger
import time
import boto3
import os
import logging

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET = "naya-finalproject-sources"
date_str = time.strftime("%Y%m%d")
datetime_str = time.strftime("%Y%m%d%H%M%S")

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

def get_date(dash=False) -> str:
    if dash:
        return time.strftime("%Y-%m-%d")
    else:
        return time.strftime("%Y%m%d")

def get_datetime() -> str:
    return time.strftime("%Y%m%d%H%M%S")

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

def get_script_name() -> str:
    return os.path.splitext(os.path.basename(__file__))[0]

def setup_logger(log_file_name) -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s',
        handlers=[
            logging.FileHandler(log_file_name, mode='w', encoding='utf-8'),
            logging.StreamHandler()  # This will print logs to stdout (Airflow log)
        ]
    )

    logger = logging.getLogger(__name__)
    logger.info(f"Logging started for {log_file_name}")
    return logger

def upload_file_to_s3(file_path: str, s3_key: str, s3) -> bool:
    """Upload a local file to S3."""
    try:
        s3.upload_file(file_path, S3_BUCKET, s3_key)
        logger.info(f"Uploaded {file_path} to S3: {s3_key}")
        return True
    except Exception as e:
        logger.error(f"Failed to upload {file_path} to S3: {e}")
        return False

def remove_local_file(file_path: str):
    try:
        os.remove(file_path)
    except Exception as e:
        logger.error(f"Could not remove local file {file_path}: {e}")
        return False
    return True

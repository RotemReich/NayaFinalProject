import boto3
import json
import requests
from elasticsearch import Elasticsearch, helpers
import os
 
# --- פרטי S3 ---
bucket_name = "naya-finalproject-json"
prefix = "PromotionDetails_json/"  # כל הקבצים בתיקייה הזו

# --- התחברות ל-Elastic ---
index_name = "chain_index"
es = Elasticsearch("http://localhost:9200")
 
# --- שלב 1: מחיקת index ישן ---
requests.delete(f"http://localhost:9200/{index_name}")
 
# --- שלב 2: יצירת index חדש ---
requests.put(
    f"http://localhost:9200/{index_name}",
    json={"settings": {"number_of_shards": 1}}
)
 
# --- S3 client ---
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
s3 = boto3.client("s3",
                  aws_access_key_id=AWS_ACCESS_KEY_ID,
                  aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
 
# --- שליפת הקבצים בתיקייה ---
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
 
def generate_actions_from_s3(key):
    """יוצר גנרטור שמחזיר מסמכים אחד-אחד ללא טעינה למזכרון"""
    obj = s3.get_object(Bucket=bucket_name, Key=key)
    for line in obj["Body"].iter_lines():   # קריאה זורמת (streaming)
        if line:
            doc = json.loads(line.decode("utf-8"))
            yield {
                "_index": index_name,
                "_source": doc,
            }
 
# --- שליחה של כל הקבצים ב-bulk (זרימה) ---
for obj in response.get("Contents", []):
    key = obj["Key"]
    if key.endswith(".json"):
        print(f"Uploading {key} ...")
        helpers.bulk(es, generate_actions_from_s3(key))
 
print("All NDJSON files uploaded successfully! 🚀")
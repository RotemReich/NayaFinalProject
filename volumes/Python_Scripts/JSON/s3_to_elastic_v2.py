import boto3
import json
import requests
from elasticsearch import Elasticsearch, helpers
import os

# --- פרטי S3 ---
bucket_name = "naya-finalproject-json"
prefix = "PromotionDetails_json/" 

# --- התחברות ל-Elastic ---
index_name = "chain_index"
es = Elasticsearch("http://localhost:9200")

# --- שלב 1: מחיקת index ישן (כדי לעדכן Mapping) ---
if es.indices.exists(index=index_name):
    requests.delete(f"http://localhost:9200/{index_name}")
    print(f"Index {index_name} deleted.")

# --- שלב 2: יצירת index חדש עם Mapping אופטימלי ---
index_config = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
    },
    "mappings": {
        "properties": {
            # שדה המיקום למפה
            "location": { "type": "geo_point" },
            
            # שדות תאריך לסינון לפי זמן
            "PromotionStartDate": { "type": "date" },
            "PromotionEndDate": { "type": "date" },
            
            # שדות קטגוריים לגרפים (Pie Charts, Bar Charts)
            "ChainNameHeb": { "type": "keyword" },
            "ChainNameEng": { "type": "keyword" },
            "StoreName": { "type": "keyword" },
            "City": { "type": "keyword" },
            "StoreID": { "type": "keyword" },
            "ChainID": { "type": "keyword" },
            
            # שדות טקסט חופשי לחיפוש
            "PromotionDescription": { "type": "text" },
            "Address": { "type": "text" },
            
            # שדות מספריים (אלסטיק יזהה אוטומטית, אבל הגדרנו ליתר ביטחון)
            "AvgItemPrice": { "type": "float" },
            "DiscountedPrice": { "type": "float" },
            "Latitude": { "type": "float" },
            "Longitude": { "type": "float" }
        }
    }
}

requests.put(f"http://localhost:9200/{index_name}", json=index_config)
print(f"Index {index_name} created with new mapping.")

# --- S3 client ---
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

s3 = boto3.client("s3",
                  aws_access_key_id=AWS_ACCESS_KEY_ID,
                  aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

# --- שליפת הקבצים בתיקייה ---
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

def generate_actions_from_s3(key):
    """גנרטור שמעבד את הנתונים לפני השליחה לאלסטיק"""
    obj = s3.get_object(Bucket=bucket_name, Key=key)
    for line in obj["Body"].iter_lines():
        if line:
            doc = json.loads(line.decode("utf-8"))
            
            # יצירת שדה ה-location מתוך השדות הקיימים ב-JSON
            if "Latitude" in doc and "Longitude" in doc:
                try:
                    doc["location"] = {
                        "lat": float(doc["Latitude"]),
                        "lon": float(doc["Longitude"])
                    }
                except (ValueError, TypeError):
                    # במקרה שהערכים אינם מספרים תקינים
                    pass
            
            yield {
                "_index": index_name,
                "_source": doc,
            }

# --- שליחה ב-bulk ---
if "Contents" in response:
    for obj in response["Contents"]:
        key = obj["Key"]
        if key.endswith(".json"):
            print(f"Uploading {key} ...")
            helpers.bulk(es, generate_actions_from_s3(key))
    print("\nAll files uploaded successfully! 🚀")
else:
    print("No files found in S3.")

print("You can now go to Kibana to create your Map and Pie Charts.")
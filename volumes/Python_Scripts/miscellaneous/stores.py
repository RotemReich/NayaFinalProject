import os
import gzip
import xml.etree.ElementTree as ET
import pandas as pd
from geopy.geocoders import ArcGIS, Nominatim
from geopy.extra.rate_limiter import RateLimiter
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import boto3
import io
from pathlib import Path
import logging
from datetime import datetime
import requests
import json
import re

# הגדרת logging - לקונסול ולקובץ
log_filename = f"geocoding_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
logger.info(f"Logging to file: {log_filename}")

# טעינת מיפוי ערים מ-data.gov.il
def fetch_city_mapping():
    """הבאת מיפוי קודים לשמות עיר מ-API של data.gov.il"""
    try:
        logger.info("Fetching city codes from data.gov.il...")
        url = "https://data.gov.il/api/3/action/datastore_search?resource_id=5c78e9fa-c2e2-4771-93ff-7f400a12f7ba&limit=10000"
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        
        data = response.json()
        city_mapping = {}
        
        if 'result' in data and 'records' in data['result']:
            for record in data['result']['records']:
                # חפש את השדות סמל_ישוב (קוד) ושם_ישוב (שם)
                code = record.get('סמל_ישוב')
                name = record.get('שם_ישוב')
                
                if code and name:
                    code_str = str(code).strip()
                    name_str = str(name).strip()
                    
                    # דלג על ערים שלא קיימות
                    if name_str.lower() not in ['לא רשום', 'לא ידוע']:
                        city_mapping[code_str] = name_str
                        logger.debug(f"Mapped city code {code_str} -> {name_str}")
        
        logger.info(f"Successfully loaded {len(city_mapping)} city codes from data.gov.il")
        return city_mapping
    
    except Exception as e:
        logger.error(f"Failed to fetch city mapping from data.gov.il: {e}")
        return {}

# הגדרת logging - לקונסול ולקובץ
log_filename = f"geocoding_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
logger.info(f"Logging to file: {log_filename}")

# S3 configuration
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
s3_client = boto3.client("s3",
                  aws_access_key_id=AWS_ACCESS_KEY_ID,
                  aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
bucket_name = "naya-finalproject-sources"
s3_source_prefix = "stores/sources/"
output_bucket_name = "naya-finalproject-processed"
s3_output_prefix = "stores/"

# Get list of files from S3
def get_s3_files(bucket, prefix):
    """Get all files from S3 prefix"""
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files = []
    if 'Contents' in response:
        for obj in response['Contents']:
            if not obj['Key'].endswith('/'):  # Skip folders
                files.append(obj['Key'])
    return files

files = get_s3_files(bucket_name, s3_source_prefix)

records = []

def read_file_from_s3(bucket, key):
    """Read file content from S3"""
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return response['Body'].read()

for file_key in files:
    logger.info(f"Processing: {file_key}")
    
    # Get file content from S3
    file_content = read_file_from_s3(bucket_name, file_key)
    
    # קובץ דחוס
    if file_key.endswith(".gz"):
        try:
            xml_content = gzip.decompress(file_content).decode('utf-8')
        except UnicodeDecodeError:
            try:
                xml_content = gzip.decompress(file_content).decode('utf-16')
            except UnicodeDecodeError:
                xml_content = gzip.decompress(file_content).decode('cp1255')
    # קובץ רגיל
    else:
        try:
            xml_content = file_content.decode('utf-8')
        except UnicodeDecodeError:
            try:
                xml_content = file_content.decode('utf-16')
            except UnicodeDecodeError:
                xml_content = file_content.decode('cp1255')

    root = ET.fromstring(xml_content)

    # --- פורמט 1 ---
    if root.find("Branches") is not None:
        for branch in root.find("Branches").findall("Branch"):
            records.append({
                "ChainID": branch.findtext("ChainID", ""),
                "SubChainID": branch.findtext("SubChainID", ""),
                "StoreID": branch.findtext("StoreID", ""),
                "ChainName": branch.findtext("ChainName", ""),
                "SubChainName": branch.findtext("SubChainName", ""),
                "StoreName": branch.findtext("StoreName", ""),
                "Address": branch.findtext("Address", ""),
                "City": branch.findtext("City", "")
            })

    # --- פורמט 2 ---
    values = root.find(".//{http://www.sap.com/abapxml}values")
    if values is not None:
        chain_id = values.findtext("CHAINID", "")
        stores = values.find("STORES")
        if stores is not None:
            for store in stores.findall("STORE"):
                records.append({
                    "ChainID": chain_id,
                    "SubChainID": store.findtext("SUBCHAINID", ""),
                    "StoreID": store.findtext("STOREID", ""),
                    "ChainName": store.findtext("CHAINNAME", ""),
                    "SubChainName": store.findtext("SUBCHAINNAME", ""),
                    "StoreName": store.findtext("STORENAME", ""),
                    "Address": store.findtext("ADDRESS", ""),
                    "City": store.findtext("CITY", "")
                })

    # --- פורמט 3: Root/SubChains/SubChain/Stores/Store ---
    if root.find("SubChains") is not None:
        chain_id = root.findtext("ChainID", "")
        chain_name = root.findtext("ChainName", "")

        for subchain in root.find("SubChains").findall("SubChain"):
            subchain_id = subchain.findtext("SubChainID", "")
            subchain_name = subchain.findtext("SubChainName", "")

            stores = subchain.find("Stores")
            if stores is not None:
                for store in stores.findall("Store"):
                    records.append({
                        "ChainID": chain_id,
                        "SubChainID": subchain_id,
                        "StoreID": store.findtext("StoreID", ""),
                        "ChainName": chain_name,
                        "SubChainName": subchain_name,
                        "StoreName": store.findtext("StoreName", ""),
                        "Address": store.findtext("Address", ""),
                        "City": store.findtext("City", "")
                    })

# יצירת DataFrame
df = pd.DataFrame(records)

# --- המרת קודים מספריים של עיר לשמות עיר ---
# טעינת מיפוי העיר מ-API
CITY_CODE_MAPPING = fetch_city_mapping()

def convert_city_code_to_name(city_value):
    """המרת קוד עיר מספרי לשם העיר"""
    if pd.isna(city_value) or city_value == "":
        return city_value
    
    city_str = str(city_value).strip()
    
    # אם זה מספר בלבד (קוד עיר), תרגם לשם
    if city_str.isdigit():
        return CITY_CODE_MAPPING.get(city_str, city_str)
    
    # אם כבר זה שם עיר, החזר כמו שהוא
    return city_str

def normalize_city_name(city_value):
    """נרמל שמות עיר — מאחד וריאציות של תל אביב ל'תל אביב - יפו' ועוד ניקוי בסיסי"""
    if pd.isna(city_value) or city_value == "":
        return city_value

    city = str(city_value).strip()

    # תאחד רווחים מרובים
    city = re.sub(r"\s+", " ", city)

    # נורמליזציה של מקפים/רווחים: החלף ' - ' או '-' ב' '
    city_clean = re.sub(r"\s*-\s*", "-", city)

    # תן מחרוזת קטנה בלי ניקוד
    city_lower = city_clean.lower()

    # אם זה תל אביב בכל וריאציה, החזר את התצוגה הרצויה
    if re.search(r"תל\s*-?\s*אביב", city_lower):
        return "תל אביב - יפו"

    # אם מכיל 'יפו' יחד עם תל אביב, גם אחזר את הנורמליזציה
    if "יפו" in city_lower and "תל" in city_lower:
        return "תל אביב - יפו"

    if "תל אבית יפה" in city_lower:
        return "תל אביב - יפו"

    # הסרת רווחי ראש/סוף שנשארו
    return city_clean

# החל את ההמרה על כל השורות: קודם המרת קודים, אחר-כך נרמול
df['City'] = df['City'].apply(convert_city_code_to_name)
df['City'] = df['City'].apply(normalize_city_name)

# df.to_csv('stores_check.csv', index=False, encoding='utf-8-sig') #לבדיקה המרת ערים

logger.info(f"Loaded {len(df)} stores. Starting geocoding...")

# --- הגדרות גיאוקודינג ---
geolocator = ArcGIS(user_agent="store_geocoder_israel", timeout=15)
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=0.3, max_retries=3, error_wait_seconds=3)

# גם Nominatim כ-fallback
nominatim_geolocator = Nominatim(user_agent="store_geocoder_israel", timeout=15)
nominatim_geocode = RateLimiter(nominatim_geolocator.geocode, min_delay_seconds=1.5, max_retries=2, error_wait_seconds=3)

def direct_geocode(address_str):
    """גיאוקודינג ללא מטמון עם fallback"""
    # נסה ArcGIS ראשון
    try:
        result = geocode(address_str)
        if result:
            return result
    except Exception as e:
        logger.debug(f"ArcGIS failed for '{address_str}': {str(e)[:100]}")
    
    # אם ArcGIS נכשל, נסה Nominatim
    try:
        logger.debug(f"Trying Nominatim for: {address_str}")
        result = nominatim_geocode(address_str)
        if result:
            return result
    except Exception as e:
        logger.debug(f"Nominatim also failed for '{address_str}': {str(e)[:100]}")
    
    return None

def is_in_israel(lat, lon):
    """בדוק אם הקואורדינטות נמצאות בישראל"""
    if lat is None or lon is None:
        return False
    israel_lat_min, israel_lat_max = 29.0, 33.5
    israel_lon_min, israel_lon_max = 34.0, 36.0
    return israel_lat_min <= lat <= israel_lat_max and israel_lon_min <= lon <= israel_lon_max

def is_valid_text(text):
    """בדוק שהטקסט מכיל לפחות אות אחת ולא רק מספרים"""
    if not text or not isinstance(text, str):
        return False
    text = text.strip()
    if not text:
        return False
    return any(c.isalpha() for c in text)

def is_online_store(row):
    """בדוק אם זו חנות אינטרנט שאין לה כתובת פיזית"""
    store_name = str(row['StoreName']).lower() if pd.notna(row['StoreName']) else ""
    address = str(row['Address']).lower() if pd.notna(row['Address']) else ""
    # Treat numeric-only city as empty
    raw_city = str(row['City']) if pd.notna(row['City']) else ""
    city = raw_city.lower() if is_valid_text(raw_city) else ""
    
    online_keywords = ['online', 'אינטרנט', 'internet', 'web', 'e-commerce', 'דיגיטלי', 'ווירטואלי', 'digital']
    
    for keyword in online_keywords:
        if keyword in store_name or keyword in address:
            return True

    address_empty = (not address) or address.strip() == ""
    city_empty = (not city) or city.strip() == ""
    store_name_empty = (not store_name) or store_name.strip() == ""

    if address_empty and city_empty:
        if store_name_empty:
            return True
        for keyword in online_keywords:
            if keyword in store_name:
                return True
        return False

    return False

def geocode_row(row):
    try:
        if is_online_store(row):
            logger.info(f"Row {row.name}: SKIPPED (online store)")
            return row.name, None, None
        
        city = str(row['City']).strip() if pd.notna(row['City']) and is_valid_text(str(row['City'])) else ""
        address = str(row['Address']).strip() if pd.notna(row['Address']) and is_valid_text(str(row['Address'])) else ""
        store_name = str(row['StoreName']).strip() if pd.notna(row['StoreName']) and is_valid_text(str(row['StoreName'])) else ""

        if (not city) and store_name:
            logger.debug(f"Row {row.name}: Promoting StoreName to City: '{store_name}'")
            city = store_name
            store_name = ""
        
        if not city and not address and not store_name:
            logger.info(f"Row {row.name}: NO DATA (missing address/city/storename)")
            return row.name, None, None
        
        queries = []
        if address and city:
            queries.append(f"{address}, {city}, Israel")
        if address and store_name:
            queries.append(f"{address}, {store_name}, Israel")
        if address:
            queries.append(f"{address}, Israel")
        if store_name and city:
            queries.append(f"{store_name}, {city}, Israel")
        if store_name:
            queries.append(f"{store_name}, Israel")
        if city:
            queries.append(f"{city}, Israel")
        
        for address_str in queries:
            location = direct_geocode(address_str)
            if location and is_in_israel(location.latitude, location.longitude):
                logger.info(f"Row {row.name}: FOUND using query '{address_str}' -> LAT:{location.latitude:.5f}, LON:{location.longitude:.5f}")
                return row.name, location.latitude, location.longitude
        
        logger.info(f"Row {row.name}: NOT FOUND (no valid results)")
        return row.name, None, None
        
    except Exception as e:
        logger.error(f"Row {row.name}: ERROR - {e}")
        return row.name, None, None

# --- גיאוקודינג עם ThreadPoolExecutor ---
output_filename = "stores_output_with_latlon.ndjson"

def save_to_s3(dataframe, bucket, prefix, filename):
    """Save DataFrame to S3 as NDJSON (newline-delimited JSON for Athena)"""
    s3_key = f"{prefix}{filename}"
    try:
        ndjson_bytes = dataframe.to_json(orient='records', force_ascii=False, lines=True).encode('utf-8-sig')
        s3_client.put_object(Bucket=bucket, Key=s3_key, Body=ndjson_bytes)
        logger.info(f"Saved NDJSON to S3: s3://{bucket}/{s3_key}")
        return
    except Exception as e:
        logger.error(f"Failed to save NDJSON to S3: {e}")
        raise

# הוסף עמודות Latitude ו Longitude אם לא קיימות
if "Latitude" not in df.columns:
    df["Latitude"] = None
if "Longitude" not in df.columns:
    df["Longitude"] = None

# עבוד על כל השורות (ללא מטמון - חישוב יומי מחדש)
logger.info(f"Processing all {len(df)} stores (no cache - recalculating daily)")

rows_to_process = [(idx, row) for idx, row in df.iterrows()]

# שימוש ב-ThreadPoolExecutor עם workers מתון
with ThreadPoolExecutor(max_workers=5) as executor:
    future_to_index = {executor.submit(geocode_row, row): idx for idx, row in rows_to_process}

    completed_count = 0
    for i, future in enumerate(as_completed(future_to_index), 1):
        idx = future_to_index[future]
        try:
            idx_result, lat, lon = future.result()
            df.at[idx, "Latitude"] = lat
            df.at[idx, "Longitude"] = lon
            completed_count += 1

            if i % 10 == 0 or i == len(rows_to_process):
                logger.info(f"Progress: {completed_count}/{len(df)} stores processed...")

        except Exception as e:
            logger.error(f"Failed to process row {idx}: {e}")

# שמור לS3 ובקובץ מקומי
save_to_s3(df, output_bucket_name, s3_output_prefix, output_filename)
df.to_json("stores_output_with_latlon.ndjson", orient='records', force_ascii=False, lines=True)

logger.info(f"Geocoding complete! Log saved to: {log_filename}")
logger.info(f"Output files saved: stores_output_with_latlon.ndjson and S3")

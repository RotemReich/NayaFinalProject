import boto3
import time
import io
import gzip
import os

# === AWS Credentials ===
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

date_prefix = time.strftime("%Y-%m-%d") + "/"

S3_SOURCE_BUCKET = "naya-finalproject-sources"

# Pre-scan S3 for corrupt .gz files (wrong magic header) and log them
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name="us-east-1",
)

# Function to check if content looks like XML
def looks_like_xml(raw: bytes) -> bool:
    head = raw[:512].lstrip()
    return bool(head) and head.startswith(b"<")

# Function to compress raw XML bytes into gzip format
def compress_xml(raw: bytes) -> bytes:
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(raw)
    return buf.getvalue()


# Pre-scan S3 for corrupt .gz files (wrong magic header) and log them
corrupt_keys = []
continuation_token = None
counter = 0
while True:
    kwargs = {"Bucket": S3_SOURCE_BUCKET, "Prefix": date_prefix}
    if continuation_token:
        kwargs["ContinuationToken"] = continuation_token
    resp = s3_client.list_objects_v2(**kwargs)
    for obj in resp.get("Contents", []):
        counter += 1
        key = obj["Key"]
        #print("Checking", key)
        if counter % 100 == 0:
            print(f"Scanned {counter} objects so far...")
        
        if not key.lower().endswith(".gz"):
            continue
        # Read first 2 bytes to verify gzip magic number 0x1f 0x8b
        try:
            rng = s3_client.get_object(Bucket=S3_SOURCE_BUCKET, Key=key, Range="bytes=0-1")
            head = rng["Body"].read(2)
            if head != b"\x1f\x8b":
                corrupt_keys.append(key)
        except Exception:
            corrupt_keys.append(key)
    if resp.get("IsTruncated"):
        continuation_token = resp.get("NextContinuationToken")
    else:
        break

print("\n", f"Found {len(corrupt_keys)} corrupt .gz files to process.")
# print("Corrupt keys: ",  corrupt_keys)

print("\n", "Starting to process corrupt files...")
# Action to take for corrupt keys
QUARANTINE_SUBPREFIX = "quarantine/"

# Process corrupt keys: try to re-gzip if body looks like XML, else quarantine
for key in corrupt_keys:
    print(f"Handling corrupt key: {key}")
    try:
        obj = s3_client.get_object(Bucket=S3_SOURCE_BUCKET, Key=key)
        body = obj["Body"].read()
    except Exception as e:
        print(f"  [ERROR] Failed to download {key}: {e}")
        continue

    # print("\n", f"body xml test: {looks_like_xml(body)}")
    # print(f"body:\n{body[:500]} ")

    if looks_like_xml(body):
        print(f">>> [FIXING] {key} looks like XML, attempting to re-compress...")
        try:
            fixed = compress_xml(body)
            s3_client.put_object(Bucket=S3_SOURCE_BUCKET, Key=key, Body=fixed, ContentType="application/gzip")
            print(f">>> [FIXED] Re-compressed XML and overwrote {key}")
            continue
        except Exception as e:
            print(f">>>>>>> [ERROR] Failed to overwrite {key}: {e}")
            # fall through to quarantine
    else:
        print(f">>> [QUARANTINE] {key} does not look like XML, moving to quarantine...")
        
        # Quarantine the corrupt file
        quarantine_key = QUARANTINE_SUBPREFIX + key
        s3_client.copy_object(Bucket=S3_SOURCE_BUCKET, CopySource=f"{S3_SOURCE_BUCKET}/{key}", Key=quarantine_key)
        print(f"  [QUARANTINED] {key} to {quarantine_key}")
        
        # Optionally delete original after quarantine
        s3_client.delete_object(Bucket=S3_SOURCE_BUCKET, Key=key)
        print(f"  [DELETED] {key}")
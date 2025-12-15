import boto3
import time
import os
from collections import defaultdict
import re

# === AWS Credentials ===

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

BUCKET = "naya-finalproject-sources"  # Change as needed
PREFIX = date_prefix = time.strftime("%Y-%m-%d") + "/"

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)

def get_all_files(bucket, prefix):
    files = []
    continuation_token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            files.append(obj["Key"])
        if resp.get("IsTruncated"):
            continuation_token = resp.get("NextContinuationToken")
        else:
            break
    return files

def group_by_subfolder(keys):
    groups = defaultdict(list)  # subfolder -> list of keys
    for key in keys:
        parts = key.split("/")
        if len(parts) > 1:
            subfolder = "/".join(parts[:-1])
            groups[subfolder].append(key)
    return groups

def get_highest_file(files: list) -> str:
    # Use dict comprehension to map extracted keys to filenames
    time_create_key = {m.group(1): x for x in files if (m := re.search(r"(\d{4})(?:\.xml)?\.gz$", x))}
    if not time_create_key:
        # fallback: keep the lexicographically highest filename
        return max(files)
    # Find the highest key and return its file
    highest_key = max(time_create_key.keys())
    return time_create_key[highest_key]

def main():
    all_files = get_all_files(BUCKET, PREFIX)
    groups = group_by_subfolder(all_files)
    for subfolder, files in groups.items():
        if len(files) > 1:
            keep = get_highest_file(files)
            to_delete = [f for f in files if f != keep]
            print(f"Subfolder: {subfolder}")
            print(f"  Keeping: {keep}")
            for f in to_delete:
                print(f"  Deleting: {f}")
                s3.delete_object(Bucket=BUCKET, Key=f)

if __name__ == "__main__":
    main()

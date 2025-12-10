import requests
from bs4 import BeautifulSoup
import boto3
import io
import time
import re
import os

BASE_URL = "https://laibcatalog.co.il/"

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET = "naya-finalproject-sources" 
S3_PREFIX = "victory-promofull-gz/"

date_str = time.strftime("%Y%m%d")
pattern = rf".*PromoFull\d+-(\d{{3}})-{date_str}.*?\.xml\.gz$"

date_prefix = time.strftime("%Y-%m-%d/")

# Fetch the HTML page
html = requests.get(BASE_URL, timeout=20)
html.raise_for_status()
soup = BeautifulSoup(html.text, "html.parser")

# Find all download links
links = soup.find_all("a", href=True, text="לחץ כאן להורדה")

PromoFull_links = []
for a in links:
    href = a["href"]
    reg = re.match(pattern, href, re.IGNORECASE)
    # if "PromoFull" in href:
    #     print("reg: ", reg)
    #     print("href: ", href)
    if reg:
        full_url = href if href.startswith("http") else BASE_URL + href.lstrip("/")
        PromoFull_links.append(full_url)

# print("Found PromoFull files:")
# for link in PromoFull_links:
#     print(" -", link)

# S3 client
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

counter = 0
print("Uploading files to S3...")
# Upload each PromoFull file to S3
for url in PromoFull_links:
    filename = url.split("/")[-1]
    branch = re.match(pattern, url, re.IGNORECASE).group(1)
    s3_key = f"{date_prefix}{S3_PREFIX}{branch}/{filename}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    with io.BytesIO(r.content) as f:
        s3.upload_fileobj(f, S3_BUCKET, s3_key)
    counter += 1
    #print(f"Uploaded Victory branch #{branch} to S3.")

print("\n\n")
print(f">>>>>>>✔ All files uploaded to S3 {S3_BUCKET} bucket!")
print(f">>>>>>>✔ Total uploaded files to '{S3_PREFIX}': {counter}")
print("\n\n")
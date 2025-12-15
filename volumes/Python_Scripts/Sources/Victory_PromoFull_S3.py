import requests
from bs4 import BeautifulSoup
import io
import time
import re
import Functions as fn

BASE_URL = "https://laibcatalog.co.il/"

S3_BUCKET = fn.get_bucket_name()
date_prefix = fn.get_date_prefix()

date_str = time.strftime("%Y%m%d")
search_word = "PromoFull"
pattern = rf".*{search_word}(\d+)-(\d{{3}})-{date_str}.*?\.xml\.gz$"


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
    if reg:
        full_url = href if href.startswith("http") else BASE_URL + href.lstrip("/")
        PromoFull_links.append(full_url)

# S3 client
s3 = fn.get_s3()
chains = fn.get_chains(s3)

counter = 0
print("Uploading files to S3...")
# Upload each PromoFull file to S3
for url in PromoFull_links:
    filename = url.split("/")[-1]
    chain_id = re.match(pattern, url, re.IGNORECASE).group(1)
    chain_name = fn.get_chain_name(chain_id, chains)
    branch_id = re.match(pattern, url, re.IGNORECASE).group(2)
    s3_key = f"{date_prefix}{search_word}/{chain_name}/{branch_id}/{filename}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    with io.BytesIO(r.content) as f:
        s3.upload_fileobj(f, S3_BUCKET, s3_key)
    counter += 1
    #print(f"Uploaded Victory branch #{branch} to S3.")

# Summary
print("\n" + "="*70)
print(f">>>>>>>✔ All files uploaded to S3 {S3_BUCKET} bucket!")
print(f">>>>>>>✔ Total uploaded files: {counter}")
print("="*70)
import os
import requests
from bs4 import BeautifulSoup
import io
import re
import Files_Assist_Functions as fn

BASE_URL = "https://laibcatalog.co.il/"

S3_BUCKET = fn.get_bucket_name()

date_prefix = fn.get_date(dash=True)
date_str = fn.get_date()
search_word = "PriceFull"
pattern = rf".*{search_word}(\d+)-(\d{{3}})-{date_str}.*?\.xml\.gz$"

# Setup logging
script_name = os.path.splitext(os.path.basename(__file__))[0]
datetime_str = fn.get_datetime()
log_file = f"{script_name}_{datetime_str}.log"
logger = fn.setup_logger(log_file)
logger.info(f"Starting Victory/Mahsanei HaShuk {search_word} script.")

# Fetch the HTML page
html = requests.get(BASE_URL, timeout=20)
html.raise_for_status()
soup = BeautifulSoup(html.text, "html.parser")

# Find all download links
links = soup.find_all("a", href=True, text="לחץ כאן להורדה")

PriceFull_links = []
for a in links:
    href = a["href"]
    reg = re.match(pattern, href, re.IGNORECASE)
    if reg:
        full_url = href if href.startswith("http") else BASE_URL + href.lstrip("/")
        PriceFull_links.append(full_url)

# S3 client
s3 = fn.get_s3()
chains = fn.get_chains(s3)

counter = 0
logger.info("Uploading files to S3...")
# Upload each PriceFull file to S3
for url in PriceFull_links:
    filename = url.split("/")[-1]
    chain_id = re.match(pattern, url, re.IGNORECASE).group(1)
    chain_name = fn.get_chain_name(chain_id, chains)
    branch_id = re.match(pattern, url, re.IGNORECASE).group(2)
    s3_key = f"{date_prefix}/{search_word}/{chain_name}/{branch_id}/{filename}"
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        with io.BytesIO(r.content) as f:
            s3.upload_fileobj(f, S3_BUCKET, s3_key)
        counter += 1
        logger.info(f"Uploaded file {filename} to {s3_key}.")
    except Exception as e:
        logger.error(f"Failed to upload {filename} from {url} to S3: {e}")

# Summary
logger.info(f">>>>>>>✔ All files uploaded to S3 {S3_BUCKET} bucket!")
logger.info(f">>>>>>>✔ Total uploaded files: {counter}")

# Upload log file to S3
if counter > 0:
    log_s3_key = f"logs/{date_prefix}/{log_file}"
    fn.upload_file_to_s3(log_file, log_s3_key, s3)  # This will upload the local log file

# Remove local log file
fn.remove_local_file(log_file)
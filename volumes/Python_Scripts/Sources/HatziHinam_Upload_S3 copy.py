from selenium import webdriver
from selenium.webdriver.chrome.service import Service
# from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
import requests
import re
import time
import boto3
import io
import os

BASE_URL = "https://shop.hazi-hinam.co.il/Prices"

# === AWS Credentials ===
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET = "naya-finalproject-sources" 
S3_PREFIX = "hatzihinam-promofull-gz/"

date_str = time.strftime("%Y%m%d")
pattern = rf"(https?://[^'\"]+PromoFull\d+-\d{{3}}-(\d{{3}})-{date_str}.*?\.gz)"

date_prefix = time.strftime("%Y-%m-%d/")

# === SETUP SELENIUM ===
options = webdriver.ChromeOptions()
options.add_argument("--headless=new")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-gpu")
options.add_argument("--remote-debugging-port=9222")
# Use preinstalled Chromium & Chromedriver in container
chrome_service = Service(executable_path="/usr/bin/chromedriver")
driver = webdriver.Chrome(service=chrome_service, options=options)

driver.get(BASE_URL)
wait = WebDriverWait(driver, 15)

# Select 'מבצעים' in the file type dropdown (use value=2 for robustness)
select = Select(wait.until(EC.presence_of_element_located((By.ID, "ddlFileType"))))
select.select_by_value("2")

# Click the search button and wait for table
search_btn = driver.find_element(By.XPATH, "//button[contains(text(), 'חיפוש')]")
search_btn.click()

all_links = set()
page = 1
visited_pages = set()
while True:
    table = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table")))
    html = driver.page_source
    page_links = re.findall(pattern, html)
    # print("page_links:", page_links)
    for url in page_links:
        all_links.add(url[0])
    print(f"Page {page}: found {len(page_links)} PromoFull .gz for today files")

    # Stop if we've already visited this page (prevents looping on last page)
    visited_pages.add(page)

    # Find numeric page 2 link and only click if we are on page 1
    if page == 1:
        page_anchors = driver.find_elements(By.CSS_SELECTOR, "div.pagination-container a.pagination-link")
        target = None
        for a in page_anchors:
            href = a.get_attribute("href") or ""
            if "p=2" in href:
                target = a
                break
        if not target:
            print("No page 2 link — stopping.")
            break

        # Click page 2 and wait for table refresh
        driver.execute_script("arguments[0].click();", target)
        try:
            wait.until(EC.staleness_of(table))
        except Exception:
            pass
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table")))
        page = 2
        continue

    # If we're already on page 2 (or higher), stop to avoid infinite loop
    print("Reached last page — stopping.")
    break

driver.quit()
print(f"Total unique .gz files: {len(all_links)} in {page} pages.")
    
# S3 client
s3 = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )
counter = 0
# Download each file
for url in all_links:
    filename = url.split("/")[-1].split("?")[0]
    #print(f"Downloading {filename}...")
    
        # Determine S3 key based on pattern
    branch = re.match(pattern, url, re.IGNORECASE).group(2)
    s3_key = f"{date_prefix}{S3_PREFIX}{branch}/{filename}"
    
    with requests.get(url, timeout=30) as r:
        r.raise_for_status()
        
        #== UPLOAD TO S3 ==
        with io.BytesIO(r.content) as f:
            s3.upload_fileobj(f, S3_BUCKET, s3_key)
    counter += 1
    #print(f"Uploaded {filename} to S3.")

print("\n\n")
print(f">>>>>>>✔ All files uploaded to S3 {S3_BUCKET} bucket!")
print(f">>>>>>>✔ Total uploaded files to '{S3_PREFIX}': {counter}")
print("\n\n")

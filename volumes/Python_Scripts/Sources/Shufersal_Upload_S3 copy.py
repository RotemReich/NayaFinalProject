import argparse
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time
import re
import boto3
import io
import os


# === CONFIG ===
BASE = "https://prices.shufersal.co.il/"

# === AWS Credentials ===
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET = "naya-finalproject-sources" 
S3_PREFIX = "shufersal-promofull-gz/"

date_str = time.strftime("%Y%m%d")
pattern = rf"(https?://[^'\"]+PromoFull\d+-(\d{{3}})-{date_str}.*?\.gz)"

date_prefix = time.strftime("%Y-%m-%d/")

# Initialize S3 client once
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)
 
HEADERS = {"User-Agent": "Mozilla/5.0"}

def extract_file_name(link):
    """Extract the file name from the given URL."""
    return link.split('/')[-1].split('?')[0]
    
def extract_promofull_hrefs(html):
    soup = BeautifulSoup(html, "html.parser")
    hrefs = []
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        m = re.match(pattern, href)
        if m and m.group(0) not in seen:
            hrefs.append(href)
            seen.add(m.group(0))
    return hrefs
 
def scrape_category_and_download(catID='4', storeId='0'): # catID='4' for PromosFull
    page = 1
    counter = 0
    top_file: str = ""
    prev_top_file: str = ""
 
    while True:
        # request specific page by adding page param
        url = urljoin(BASE, 'FileObject/UpdateCategory')
        params = {'catID': str(catID), 'storeId': str(storeId), 'page': str(page)}
        #print(f"Fetching category grid: {url} params={params}")
        r = requests.get(url, params=params, headers=HEADERS, timeout=30)
        r.raise_for_status()
        html = r.text
        
        links = extract_promofull_hrefs(html)
        if links == []:
            continue
        try:
            top_file = extract_file_name(links[0])
            #print("Top file on this page: ", top_file)
            if top_file == prev_top_file:
                print("Last page reached. End pagination.")
                break
            prev_top_file = top_file
        except Exception as e:
            print("Pagination end detection error:", e)
        print(f"Page {page}: found {len(links)} PromoFull links for today files")
        
        # filter only PromoFull blob links (the extractor already does that)
        for u in links:
            reg_u = re.match(pattern, u)
            #print(">>>>>>>>>> reg_u: ", reg_u.groups())
            filename = extract_file_name(u)
            s3_key = f"{date_prefix}{S3_PREFIX}{reg_u.group(2)}/{filename}"
            # print(f"Uploading -> s3://{S3_BUCKET}/{s3_key}")
            try:
                with requests.get(u, headers=HEADERS, timeout=60) as r:
                    r.raise_for_status()
                    
                    #== UPLOAD TO S3 ==
                    with io.BytesIO(r.content) as f:
                        s3.upload_fileobj(f, S3_BUCKET, s3_key)
                    counter += 1
                # print(f"✔ Uploaded: {filename}")
            except Exception as e:
                print("Upload error:", e)
            # polite pause
        time.sleep(1)
        
        print("Uploaded count so far: ", counter)
        page += 1
    
    print("\n")
    print(f">>>>>>>✔ All files uploaded to S3 {S3_BUCKET} bucket!")
    print(f">>>>>>>✔ Total uploaded files to '{S3_PREFIX}': {counter}")
    print("\n\n")


def main():
    p = argparse.ArgumentParser(description='Download Shufersal PromoFull files')
    p.add_argument('--cat', default='4', help='category id (default 4 = PromosFull)')
    p.add_argument('--store', default='0', help='store id (default 0 = all)')
    args = p.parse_args()
    scrape_category_and_download(catID=args.cat, storeId=args.store)


if __name__ == '__main__':
    main()
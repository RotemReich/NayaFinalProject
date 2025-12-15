import argparse
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time
import re
import io
import Functions as fn


# === CONFIG ===
BASE = "https://prices.shufersal.co.il/"

# === AWS Credentials ===
S3_BUCKET = fn.get_bucket_name()

date_str = time.strftime("%Y%m%d")
search_word = "PriceFull"
cat_IDs = fn.shufersal_catIDs
pattern = rf"(https?://[^'\"]+{search_word}(\d+)-(\d{{3}})-{date_str}.*?\.gz)"

date_prefix = fn.get_date_prefix()

# Initialize S3 client once
s3 = fn.get_s3()
chains = fn.get_chains(s3)

HEADERS = {"User-Agent": "Mozilla/5.0"}

def extract_file_name(link):
    """Extract the file name from the given URL."""
    return link.split('/')[-1].split('?')[0]

def extract_pricefull_hrefs(html):
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

def scrape_category_and_download(catID=cat_IDs[search_word], storeId='0'): # catID='2' for PricesFull
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

        links = extract_pricefull_hrefs(html)
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
        print(f"Page {page}: found {len(links)} {search_word} links for today files")

        # filter only PriceFull blob links (the extractor already does that)
        for u in links:
            reg_u = re.match(pattern, u, re.IGNORECASE)
            chain_id = reg_u.group(2)
            chain_name = fn.get_chain_name(chain_id, chains)
            branch_id = reg_u.group(3)
            filename = extract_file_name(u)
            s3_key = f"{date_prefix}{search_word}/{chain_name}/{branch_id}/{filename}"
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
    
    # Summary
    print("\n" + "="*70)
    print(f">>>>>>>✔ All files uploaded to S3 {S3_BUCKET} bucket!")
    print(f">>>>>>>✔ Total uploaded: {counter}")
    print("="*70)


def main():
    p = argparse.ArgumentParser(description=f'Download Shufersal {search_word} files')
    p.add_argument('--cat', default=cat_IDs[search_word], help=f"category id (default {cat_IDs[search_word]} = {search_word})")
    p.add_argument('--store', default='0', help='store id (default 0 = all)')
    args = p.parse_args()
    scrape_category_and_download(catID=args.cat, storeId=args.store)


if __name__ == '__main__':
    main()
import os
import requests
import re
import time
import argparse
from urllib.parse import urljoin
import Files_Assist_Functions as fn

BASE_URL = "https://prices.carrefour.co.il/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}

date_prefix = fn.get_date(dash=True)
today = fn.get_date()

S3_BUCKET = fn.get_bucket_name()
s3 = fn.get_s3()
chains = fn.get_chains(s3)
search_word = "PromoFull"

# Setup logging
script_name = os.path.splitext(os.path.basename(__file__))[0]
datetime_str = fn.get_datetime()
log_file = f"{script_name}_{datetime_str}.log"
logger = fn.setup_logger(log_file)
logger.info(f"Starting Carrefour {search_word} script.")

def extract_promofull_files(html_content):
    files = {}
    # Use a pattern that matches the full filename in the HTML
    pattern = rf"{search_word}(\d+)-(\d{{4}})-({today})(\d{{4}})\.gz"
    filename_pattern = rf"{search_word}\d+-\d{{4}}-{today}\d{{4}}\.gz"
    filenames = re.findall(filename_pattern, html_content)
    for filename in filenames:
        m = re.match(pattern, filename)
        if not m:
            continue
        chain_id, branch_id = m.group(1), m.group(2)
        file_url = urljoin(BASE_URL, f"{today}/{filename}")
        files[filename] = {
            "filename": filename,
            "url": file_url,
            "chain_id": chain_id,
            "branch_id": branch_id,
        }
    return list(files.values())

def upload_to_s3(url, s3_key):
    """Upload a file directly from a URL to S3 without saving locally."""
    try:
        response = requests.get(url, headers=HEADERS, timeout=60, stream=True)
        response.raise_for_status()
        s3.upload_fileobj(response.raw, S3_BUCKET, s3_key)
        return True
    except Exception as e:
        logger.error(f"Failed to upload from {url} to S3: {e}")
        return False

def main(dry_run=False):
    """Main function"""
    session = requests.Session()
    all_files = {}
    try:
        response = session.get(BASE_URL, headers=HEADERS, timeout=15)
        logger.info(f"Fetched BASE_URL: {BASE_URL} (status {response.status_code})")
        response.raise_for_status()
        html_content = response.text
        current_files = extract_promofull_files(html_content)
        for file_info in current_files:
            all_files[file_info["filename"]] = file_info      
        if not all_files:
            logger.warning("No files found to process.")
            return
        
        if dry_run:
            for i, (filename, file_info) in enumerate(all_files.items(), 1):
                logger.info(f"[DRY RUN] [{i}/{len(all_files)}] {filename} -> {file_info['url']}")
            return

        logger.info("Uploading files to S3...")
        uploaded = 0
        upload_failed = 0
        for i, (filename, file_info) in enumerate(all_files.items(), 1):
            chain_id = file_info["chain_id"]
            chain_name = fn.get_chain_name(chain_id, chains)
            branch_id = file_info["branch_id"]
            s3_key = f"{date_prefix}/{search_word}/{chain_name}/{branch_id}/{filename}"
            # Upload directly from the internet
            if upload_to_s3(file_info["url"], s3_key):
                uploaded += 1
                logger.info(f"Uploaded: {filename} to {s3_key}")
            else:
                upload_failed += 1
                logger.error(f"Failed to upload: {filename} to {s3_key}")
            # if uploaded % 10 == 0:
            #     logger.info(f">>> Uploaded {uploaded} files so far...")
            time.sleep(0.3)
        
        # Summary
        logger.info(f"All files uploaded to S3 {S3_BUCKET} bucket!")
        logger.info(f"Total uploaded files: {uploaded}")
        logger.info(f"Failed uploads: {upload_failed}")

        # Upload log file to S3
        if uploaded > 0 or upload_failed > 0:
            log_s3_key = f"logs/{date_prefix}/{log_file}"
            fn.upload_file_to_s3(log_file, log_s3_key, s3)  # This will upload the local log file

    except Exception as e:
        logger.exception(f"Exception occurred: {e}")
    
    finally:
        try:
            session.close()
        except Exception as e:
            print(f"Error occurred while closing session: {e}")
        
        # Remove local log file
        fn.remove_local_file(log_file)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=f"Carrefour {search_word} downloader")
    parser.add_argument('--dry-run', action='store_true', help='List found files without downloading')
    args = parser.parse_args()
    main(dry_run=args.dry_run)
import requests
import json
import re
import os
import time
import argparse
from urllib.parse import urljoin

BASE_URL = "https://prices.carrefour.co.il/"
DOWNLOAD_FOLDER = "carrefour_downloaded_promofull_files"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
}

if not os.path.exists(DOWNLOAD_FOLDER):
    os.makedirs(DOWNLOAD_FOLDER)

def extract_date_folders(html_content):
    """Extract available date folders"""
    pattern = r'const date_folders = JSON\.parse\(`\[(.*?)\]`\)'
    match = re.search(pattern, html_content)
    if not match:
        return []
    json_str = '[' + match.group(1) + ']'
    try:
        dates = json.loads(json_str)
        converted_dates = []
        for date_str in dates:
            parts = date_str.split('/')
            if len(parts) == 3:
                day, month, year = parts
                converted = f"{year}{month}{day}"
                converted_dates.append(converted)
        return converted_dates
    except Exception:
        return []

def extract_promofull_files(html_content, base_date=None):
    files = {}
    pattern = r'PromoFull(\d+)-(\d{4})-(\d{8})\d{4}\.gz'
    matches = re.findall(pattern, html_content)
    # To get the full filename, search for all occurrences
    filename_pattern = r'PromoFull\d+-\d{4}-\d{8}\d{4}\.gz'
    filenames = re.findall(filename_pattern, html_content)
    for filename in filenames:
        m = re.match(pattern, filename)
        if not m:
            continue
        chain_code, store_code, file_date = m.group(1), m.group(2), m.group(3)
        date_folder = file_date if file_date else (base_date or '')
        if not date_folder:
            continue
        file_url = urljoin(BASE_URL, f"{date_folder}/{filename}")
        files[filename] = {
            'filename': filename,
            'url': file_url,
            'date': date_folder,
            'chain_code': chain_code,
            'store_code': store_code,
            'file_date': file_date
        }
    return list(files.values())

def download_file(url, filename, session=None):
    """Download a single file"""
    filepath = os.path.join(DOWNLOAD_FOLDER, filename)
    if os.path.exists(filepath):
        return True
    try:
        if session:
            response = session.get(url, headers=HEADERS, timeout=60, stream=True)
        else:
            response = requests.get(url, headers=HEADERS, timeout=60, stream=True)
        response.raise_for_status()
        downloaded = 0
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
        return True
    except Exception:
        if os.path.exists(filepath):
            os.remove(filepath)
        return False

def main(dry_run=False):
    """Main function"""
    session = requests.Session()
    all_files = {}
    try:
        response = session.get(BASE_URL, headers=HEADERS, timeout=15)
        response.raise_for_status()
        html_content = response.text
        current_files = extract_promofull_files(html_content)
        for file_info in current_files:
            all_files[file_info['filename']] = file_info
        date_folders = extract_date_folders(html_content)
        for date in date_folders:
            try:
                print(f"Checking date folder: {date}")
                page_url = urljoin(BASE_URL, f"{date}/")
                r = session.get(page_url, headers=HEADERS, timeout=15)
                if r.status_code != 200:
                    continue
                page_html = r.text
                date_files = extract_promofull_files(page_html, base_date=date)
                for f in date_files:
                    all_files[f['filename']] = f
                time.sleep(0.2)
            except Exception:
                pass
        if not all_files:
            return
        success = 0
        failed = 0
        if dry_run:
            for i, (filename, file_info) in enumerate(all_files.items(), 1):
                print(f"[{i}/{len(all_files)}] {filename} -> {file_info['url']}")
            return
        for i, (filename, file_info) in enumerate(all_files.items(), 1):
            if download_file(file_info['url'], file_info['filename'], session):
                success += 1
            else:
                failed += 1
            time.sleep(0.3)
        # Summary
        print("\n" + "="*70)
        print("Download Complete:")
        print(f"  Successfully downloaded: {success}")
        print(f"  Failed: {failed}")
        print(f"  Total: {len(all_files)}")
        print(f"  Location: {os.path.abspath(DOWNLOAD_FOLDER)}")
        print("="*70)
    except Exception:
        pass
    finally:
        session.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Carrefour PromoFull downloader')
    parser.add_argument('--dry-run', action='store_true', help='List found files without downloading')
    args = parser.parse_args()
    main(dry_run=args.dry_run)
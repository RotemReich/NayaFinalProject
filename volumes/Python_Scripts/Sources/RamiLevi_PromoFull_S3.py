import time
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException
import io
import re
import urllib3
import Functions as fn

# Suppress only the single InsecureRequestWarning from urllib3 needed.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# === CONFIG ===
LOGIN_URL = "https://url.publishedprices.co.il/login"
USERNAME = "RamiLevi"

# === SETUP SELENIUM ===
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--remote-debugging-port=9222")
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0 Safari/537.36")

# Initialize webdriver; will fail if Chromedriver/Chromium isn't installed in the container.
driver = webdriver.Chrome(options=chrome_options)

# === AWS Credentials ===
# Use environment variables injected by Docker Compose. Do NOT hardcode secrets.
S3_BUCKET = fn.get_bucket_name()
date_prefix = fn.get_date_prefix()

date_str = time.strftime("%Y%m%d")
search_word = "PromoFull"
pattern = rf".*{search_word}(\d+)-(\d{{3}})-{date_str}.*?\.gz$"


try:
    # Open login page
    driver.get(LOGIN_URL)
    
    # Wait for the username input to be present
    wait = WebDriverWait(driver, 20)
    username_input = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='username'], input[type='text']")))
    
    # Enter username
    username_input.clear()
    username_input.send_keys(USERNAME)
    
    # Password field (if any) - skip since password is empty, but just to find and clear if present
    try:
        password_input = driver.find_element(By.CSS_SELECTOR, "input[type='password']")
        password_input.clear()
    except Exception:
        pass
    
    # Submit the form (press Enter on username or find the Sign In button)
    username_input.send_keys(Keys.RETURN)
    
    # Wait for redirect or page load after login
    wait.until(EC.url_contains("/file"))  # Wait until URL contains "/file"
    # Wait until table is present
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#fileList tbody")))

    # Filter the list using the Find bar: type "PromoFull"
    try:
        search_box = wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "input[type='search'][aria-controls='fileList']")
            )
        )
        search_box.clear()
        search_box.send_keys(search_word)
        # wait for anchors count to stabilize
        def _count_anchors(driver):
            return len(driver.find_elements(By.CSS_SELECTOR, "#fileList tbody tr a[href$='.gz']"))
        stable_checks = 0
        last_count = -1
        start_ts = time.time()
        while time.time() - start_ts < 6:  # up to ~6s stabilization
            count = _count_anchors(driver)
            if count == last_count:
                stable_checks += 1
            else:
                stable_checks = 0
            last_count = count
            if stable_checks >= 3:  # ~3 consecutive same counts
                break
            time.sleep(0.5)
    except Exception as e:
        print("Search filter not applied:", e)

    # Scrape filtered .gz links from the file table specifically
    gz_links = []
    try:
        # Re-query rows to avoid stale elements
        rows = driver.find_elements(By.CSS_SELECTOR, "#fileList tbody tr")
        for r in rows:
            try:
                anchors = r.find_elements(By.CSS_SELECTOR, "a[href$='.gz']")
                for a in anchors:
                    href = a.get_attribute("href")
                    if href:
                        gz_links.append(href)
            except StaleElementReferenceException:
                # Re-acquire the row/anchors if table refreshed
                anchors = driver.find_elements(By.CSS_SELECTOR, "#fileList tbody tr a[href$='.gz']")
                for a in anchors:
                    href = a.get_attribute("href")
                    if href:
                        gz_links.append(href)
    except Exception as e:
        print("Failed to read table rows:", e)

    # De-duplicate
    gz_links = list({h for h in gz_links})
    print(f'Found {len(gz_links)} files after filter "{search_word}".')
    
    # Copy cookies ONCE and close the browser before downloads to prevent invalid session id
    cookie_jar = requests.cookies.RequestsCookieJar()
    try:
        for cookie in driver.get_cookies():
            cookie_jar.set(cookie['name'], cookie['value'], domain=cookie.get('domain'), path=cookie.get('path'))
    except Exception as e:
        print(f"Warning: could not extract cookies: {e}")
    finally:
        try:
            driver.quit()
        except Exception:
            pass
        driver = None
    
    # S3 client
    s3 = fn.get_s3()
    chains = fn.get_chains(s3)
    
    counter = 0
    skipped = 0
    print("Uploading files to S3...")
    # Use a single requests session with the copied cookies
    session = requests.Session()
    session.cookies.update(cookie_jar)
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0 Safari/537.36",
        "Accept": "*/*",
        # Prevent transparent decompression that can corrupt raw bytes in some environments
        "Accept-Encoding": "identity",
    })

    def _http_get_with_retry(sess, url, max_retries=3, backoff=2):
        last_err = None
        for attempt in range(1, max_retries + 1):
            try:
                resp = sess.get(url, timeout=90, verify=False, stream=True, allow_redirects=True)
                resp.raise_for_status()
                return resp
            except Exception as e:
                last_err = e
                if attempt < max_retries:
                    time.sleep(backoff * attempt)
        raise last_err
    
    for url in gz_links:
        
        #== Build S3 key
        filename = url.split("/")[-1].split("?")[0]
        m = re.match(pattern, filename, re.IGNORECASE)
        if not m:
            skipped += 1
            continue
        chain_id = m.group(1)
        chain_name = fn.get_chain_name(chain_id, chains)
        branch_id = m.group(2)
        s3_key = f"{date_prefix}{search_word}/{chain_name}/{branch_id}/{filename}"

        #== Upload to S3
        try:
            r = _http_get_with_retry(session, url)
            # Skip HTML responses (auth pages, errors)
            ct = (r.headers.get("Content-Type") or "").lower()
            if "text/html" in ct:
                print(f"Skip HTML response for {filename} (Content-Type: {ct})")
                skipped += 1
                # Drain to release connection
                try:
                    for _ in r.iter_content(chunk_size=1024*16):
                        pass
                except Exception:
                    pass
                continue
            expected_len = int(r.headers.get("Content-Length", "0") or 0)

            buf = io.BytesIO()
            for chunk in r.iter_content(chunk_size=1024 * 64):
                if chunk:
                    buf.write(chunk)
            size = buf.tell()
            if expected_len and size != expected_len:
                print(f"Size mismatch for {filename}: got {size}, expected {expected_len}")
            buf.seek(0)

            # Upload raw bytes; tag as gzip content
            extra = {"ContentType": "application/gzip"}
            # Do not set ContentEncoding unless you want S3 to auto-decompress on download; keep raw bytes
            s3.upload_fileobj(buf, S3_BUCKET, s3_key, ExtraArgs=extra)
            counter += 1
        except Exception as e:
            print(f"Failed to upload {filename}: {e}")
finally:
    print("\n" + "="*70)
    print(f">>>>>>>✔ All files uploaded to S3 {S3_BUCKET} bucket!")
    print(f">>>>>>>✔ Total uploaded files: {counter}")
    print(f">>>>>>>✔ Total skipped files: {skipped}")
    print("="*70)
    
    try:
        if 'driver' in locals() and driver is not None:
            driver.quit()
    except Exception:
        pass

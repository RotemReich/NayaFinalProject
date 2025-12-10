import os
import time
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# === CONFIG ===
LOGIN_URL = "https://url.publishedprices.co.il/login"
USERNAME = "RamiLevi"
script_dir = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(script_dir, "RamiLevi_PromoFull_Files")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# === SETUP SELENIUM ===
chrome_options = Options()
chrome_options.add_argument("--headless")  # Run in background without opening a window
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")

driver = webdriver.Chrome(options=chrome_options)

try:
    # Open login page
    driver.get(LOGIN_URL)
    
    # Wait for the username input to be present
    wait = WebDriverWait(driver, 10)
    username_input = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='username'], input[type='text']")))
    
    # Enter username
    username_input.send_keys(USERNAME)
    
    # Password field (if any) - skip since password is empty, but just to find and clear if present
    try:
        password_input = driver.find_element(By.CSS_SELECTOR, "input[type='password']")
        password_input.clear()
    except:
        pass
    
    # Submit the form (press Enter on username or find the Sign In button)
    username_input.send_keys(Keys.RETURN)
    
    # Wait for redirect or page load after login
    wait.until(EC.url_contains("/file"))  # Wait until URL contains "/file"
    time.sleep(3)  # Additional wait to be safe
    
    # Scrape all file links with .gz extension
    links = driver.find_elements(By.TAG_NAME, "a")
    gz_links = [link.get_attribute("href") for link in links if link.get_attribute("href") and link.get_attribute("href").endswith(".gz")]
    
    print(f"Found {len(gz_links)} files.")
    
    # Download the files using requests to keep it simple
    session = requests.Session()
    for url in gz_links:
        filename = url.split("/")[-1].split("?")[0]
        if "PROMOFULL" not in filename.upper():
            continue
        
        filepath = os.path.join(OUTPUT_DIR, filename)
        print(f"Downloading {filename} ...")
        try:
            # Optional: copy cookies from selenium to requests if needed
            for cookie in driver.get_cookies():
                session.cookies.set(cookie['name'], cookie['value'])
            
            r = session.get(url, timeout=60, verify=False)
            r.raise_for_status()
            with open(filepath, "wb") as f:
                f.write(r.content)
            print(f"Downloaded {filename}")
        except Exception as e:
            print(f"Failed to download {filename}: {e}")
finally:
    driver.quit()

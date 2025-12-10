import requests
from bs4 import BeautifulSoup
import os

BASE_URL = "https://laibcatalog.co.il/"

script_dir = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(script_dir, "victory_PromoFull_Files")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Fetch the HTML page
html = requests.get(BASE_URL, timeout=20)
html.raise_for_status()
soup = BeautifulSoup(html.text, "html.parser")

# Find all download links
links = soup.find_all("a", href=True, text="לחץ כאן להורדה")

PromoFull_links = []
for a in links:
    href = a["href"]
    if "PromoFull" in href:
        full_url = href if href.startswith("http") else BASE_URL + href.lstrip("/")
        PromoFull_links.append(full_url)

print("Found PromoFull files:")
for link in PromoFull_links:
    print(" -", link)

# Download each PromoFull file
for url in PromoFull_links:
    filename = url.split("/")[-1]
    dest_path = os.path.join(OUTPUT_DIR, filename)

    print(f"Downloading {filename}...")
    r = requests.get(url, timeout=30)
    r.raise_for_status()

    with open(dest_path, "wb") as f:
        f.write(r.content)

print("✔ All PromoFull files downloaded!")
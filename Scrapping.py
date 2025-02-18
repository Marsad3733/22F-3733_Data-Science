import aiohttp
import asyncio
from bs4 import BeautifulSoup
import os
import re
import csv
import json
import sys

# Force UTF-8 encoding to prevent UnicodeEncodeError
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

TIMEOUT = aiohttp.ClientTimeout(total=300)
DOWNLOAD_DIR = "C:/NeurIPS_papers"
CSV_FILE = os.path.join(DOWNLOAD_DIR, "metadata.csv")
JSON_FILE = os.path.join(DOWNLOAD_DIR, "metadata.json")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

def store_csv(metadata):
    file_exists = os.path.isfile(CSV_FILE)
    with open(CSV_FILE, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=['year', 'title', 'authors', 'abstract', 'pdf_url'])
        if not file_exists:
            writer.writeheader()
        writer.writerow(metadata)
    print(f"[INFO] Data stored in CSV: {CSV_FILE}")

def store_json(metadata):
    if os.path.exists(JSON_FILE):
        with open(JSON_FILE, 'r+', encoding='utf-8') as file:
            try:
                data = json.load(file)
            except json.JSONDecodeError:
                data = []
            data.append(metadata)
            file.seek(0)
            json.dump(data, file, indent=4)
    else:
        with open(JSON_FILE, 'w', encoding='utf-8') as file:
            json.dump([metadata], file, indent=4)
    print(f"[INFO] Data stored in JSON: {JSON_FILE}")

def clean_filename(name):
    return re.sub(r'[\/\\:*?"<>|]', '', name)[:200]

def get_processed_titles():
    if os.path.exists(JSON_FILE):
        with open(JSON_FILE, "r", encoding="utf-8") as file:
            try:
                return {entry["title"] for entry in json.load(file)}
            except json.JSONDecodeError:
                return set()
    return set()

async def process_document(session, doc_url, year, processed_titles):
    print(f"[INFO] Handling document: {doc_url}")  
    async with session.get(doc_url) as response:
        html_content = await response.text()
        page = BeautifulSoup(html_content, 'html.parser')
        title_element = page.select_one('title')
        
        if title_element:
            title = clean_filename(title_element.get_text().strip())
        else:
            print(f"[WARNING] Title missing for {doc_url}")
            return
        
        if title in processed_titles:
            print(f"[INFO] Skipping previously processed document: {title}")
            return  
        
        pdf_element = page.select_one('a.btn[href*="Paper.pdf"]') or page.select_one('a.btn[href*="Paper-Conference.pdf"]')
        pdf_url = f"https://papers.nips.cc{pdf_element['href']}" if pdf_element else None
        
        if pdf_url:
            print(f"[INFO] PDF detected for: {title} -> {pdf_url}")
            await download_pdf(session, pdf_url, title) 
        else:
            print(f"[WARNING] No PDF located for {title}")
        
        # Handling authors extraction properly
        authors_element = page.find("h4", text="Authors")
        authors = authors_element.find_next_sibling("p").get_text(strip=True) if authors_element else "Authors not available"

        # Handling abstract extraction properly
        abstract_element = page.find("h4", text="Abstract")
        abstract = abstract_element.find_next_sibling("p").get_text(strip=True) if abstract_element else "Abstract not available"

        metadata = {
            'year': year,
            'title': title,
            'authors': authors,
            'abstract': abstract,
            'pdf_url': pdf_url if pdf_url else "Unavailable"
        }
        
        store_csv(metadata)
        store_json(metadata)

async def download_pdf(session, pdf_url, filename, attempts=3):
    print(f"[INFO] Initiating PDF download: {pdf_url}")
    file_path = os.path.join(DOWNLOAD_DIR, f"{filename}.pdf")
    
    if os.path.exists(file_path):
        print(f"[INFO] PDF already downloaded: {file_path}")
        return
    
    for attempt in range(attempts):
        try:
            async with session.get(pdf_url, timeout=TIMEOUT) as response:
                if response.status == 200:
                    with open(file_path, 'wb') as file:
                        while chunk := await response.content.read(1024 * 1024):
                            file.write(chunk)
                    print(f"[SUCCESS] PDF stored: {file_path}")
                    return  
                else:
                    print(f"[WARNING] Failed PDF request {pdf_url} (HTTP {response.status})")
        except Exception as e:
            print(f"[ERROR] Issue downloading {pdf_url} attempt {attempt + 1}: {e}")
        if attempt < attempts - 1:
            print(f"[INFO] Retrying download ({attempt + 1}/{attempts})...")
            await asyncio.sleep(5)
    print(f"[ERROR] Failed to retrieve {pdf_url} after {attempts} attempts")

async def fetch(session, url, retries=3):
    print(f"[INFO] Fetching URL: {url}") 
    for attempt in range(retries):
        try:
            async with session.get(url, timeout=TIMEOUT) as response:
                if response.status == 200:
                    html = await response.text()
                    print(f"[INFO] Successfully fetched: {url}")
                    return html
                else:
                    print(f"[WARNING] Failed to fetch {url} (HTTP {response.status})")

        except asyncio.TimeoutError:
            print(f"[ERROR] Timeout while fetching {url}, attempt {attempt + 1}/{retries}")
        except Exception as e:
            print(f"[ERROR] Network error: {e}, attempt {attempt + 1}/{retries}")

        if attempt < retries - 1:
            print(f"[INFO] Retrying fetch ({attempt + 1}/{retries})...")
            await asyncio.sleep(5)

    print(f"[ERROR] Giving up on {url} after {retries} attempts")
    return None

async def fetch_yearly_data(year):
    base_url = f"https://papers.nips.cc/paper_files/paper/{year}"
    print(f"[INFO] Retrieving data for: {year}") 
    async with aiohttp.ClientSession() as session:
        html_content = await fetch(session, base_url)
        if not html_content:
            print(f"[ERROR] Failed to load {year}, skipping...")
            return
        
        page = BeautifulSoup(html_content, 'html.parser')
        papers = page.select("ul.paper-list li a[href*='-Abstract']")
        print(f"[INFO] Located {len(papers)} papers for {year}")
        processed_titles = get_processed_titles()
        
        for paper in papers:
            title = paper.get_text(strip=True)
            doc_url = f"https://papers.nips.cc{paper['href']}"
            if title in processed_titles:
                print(f"[INFO] Skipping previously processed document: {title}")
                continue
            print(f"[INFO] Identified document: {title.encode('utf-8', 'ignore').decode('utf-8')} -> {doc_url}")
            await process_document(session, doc_url, year, processed_titles)

async def main():
    years = range(2018, 2024)
    print("[INFO] Starting data retrieval...")
    async with aiohttp.ClientSession() as session:
        for year in years:
            await fetch_yearly_data(year)
            await asyncio.sleep(5)
    print("[INFO] Data retrieval completed!")

if __name__ == '__main__':

    asyncio.run(main())
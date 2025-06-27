import csv
import json
import os
import re
import threading
from pathlib import Path
from typing import Dict, Any, Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from playwright.sync_api import sync_playwright
import requests
from bs4 import BeautifulSoup

CSV_PATH = "Broward_County.csv"
TRACKED_CSV_PATH = "Broward_County_tracking.csv"
OUT_DIR = Path("broward")
DOCS_DIR = Path("documents")
SEARCH_URL = "https://web.bcpa.net/BcpaClient/search.aspx/GetData"
PARCEL_URL = "https://web.bcpa.net/BcpaClient/search.aspx/getParcelInformation"
SALES_URL = "https://web.bcpa.net/BcpaClient/search.aspx/getRecentSalesList"

HEADERS = {
    "Content-Type": "application/json; charset=UTF-8",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://web.bcpa.net",
    "Referer": "https://web.bcpa.net/BcpaClient/"
}

LOCK = threading.Lock()

def strip_state_zip_country(google_addr: str) -> str:
    street_city = ",".join(google_addr.split(",")[:2]).strip().upper()
    street_city = re.sub(r"\bCOURT?\b", "CT", street_city)
    street_city = re.sub(r"\bAV(?:ENUE)?\b", "AVE", street_city)
    street_city = re.sub(r"\bST(?:REET)?\b", "ST", street_city)
    return street_city

def search_address(value: str) -> Optional[str]:
    payload = {
        "value": value,
        "cities": "",
        "orderBy": "NAME",
        "pageNumber": "1",
        "pageCount": "5000",
        "arrayOfValues": "",
        "selectedFromList": "false",
        "totalCount": "Y",
    }
    r = requests.post(SEARCH_URL, headers=HEADERS, json=payload, timeout=30)
    r.raise_for_status()
    data = r.json()["d"]
    if data["TotalAmountOfRecordsFoundk__BackingField"] == 0:
        return None
    return data["resultListk__BackingField"][0]["folioNumber"]

def fetch_parcel_info(folio: str) -> Dict[str, Any]:
    payload = {
        "folioNumber": folio,
        "taxyear": "2025",
        "action": "CURRENT",
        "use": ""
    }
    r = requests.post(PARCEL_URL, headers=HEADERS, json=payload, timeout=60)
    r.raise_for_status()
    return r.json()["d"]

def fetch_recent_sales(folio: str, use_code: str) -> List[Dict[str, Any]]:
    payload = {
        "folioNumber": folio,
        "useCode": use_code.split("-")[0].strip(),
        "count": "99"
    }
    r = requests.post(SALES_URL, headers=HEADERS, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()["d"]

def fetch_pdf_url_from_cin(cin: str) -> Optional[str]:
    url = f"https://officialrecords.broward.org/AcclaimWeb/Details/GetDocumentbyInstrumentNumber/O/{cin}"
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(url, timeout=60000, wait_until="domcontentloaded")
            page.wait_for_timeout(8000)

            token = page.locator("input#hdnTransactionItemId").get_attribute("value")
            browser.close()

            if not token:
                print(f"⚠️ CIN {cin}: No transaction token found.")
                return None

            return f"https://officialrecords.broward.org/AcclaimWeb/Image/DocumentPdfAllPages/{token}"
    except Exception as e:
        print(f"❌ CIN {cin} - Playwright error: {e}")
        return None

def download_pdf(url: str, save_path: Path):
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    with open(save_path, "wb") as f:
        f.write(r.content)

def safe_filename(name: str) -> str:
    return re.sub(r"[^\w\-_.() ]", "_", name).strip().replace("  ", " ")

def process_row(index: int, row: dict, results: List[bool]):
    orig_addr = row["original_address"].strip()
    google_addr = row["google_formatted_address"].strip()
    value = strip_state_zip_country(google_addr)

    print(f"🔍 [{index}] Searching: {value}")

    try:
        folio = search_address(value)
        if not folio:
            print(f"❌ [{index}] No records for: {orig_addr}")
            results[index] = False
            return

        parcel_json = fetch_parcel_info(folio)
        use_code = parcel_json["parcelInfok__BackingField"][0]["useCode"]
        recent_sales = fetch_recent_sales(folio, use_code)
        parcel_json["recentSales_from_api"] = recent_sales

        doc_folder = DOCS_DIR / safe_filename(orig_addr)
        doc_folder.mkdir(parents=True, exist_ok=True)

        all_cins = {
            sale["cin"] for sale in parcel_json.get("recentSalesk__BackingField", []) if sale.get("cin")
        } | {
            sale["cin"] for sale in recent_sales if sale.get("cin")
        }

        # Also include bookAndPageOrCin1 from parcel info if present
        book_cin = parcel_json.get("parcelInfok__BackingField", [{}])[0].get("bookAndPageOrCin1")
        if book_cin:
            all_cins.add(book_cin)

        parcel_json["documents_info"] = []

        def download_cin(cin: str):
            pdf_url = fetch_pdf_url_from_cin(cin)
            if not pdf_url:
                return
            save_path = doc_folder / f"{cin}.pdf"
            download_pdf(pdf_url, save_path)
            with LOCK:
                parcel_json["documents_info"].append({
                    "cin": cin,
                    "pdf_url": pdf_url,
                    "local_path": str(save_path)
                })

        with ThreadPoolExecutor(max_workers=10) as pdf_executor:
            pdf_executor.map(download_cin, all_cins)

        json_path = OUT_DIR / f"{safe_filename(orig_addr)}.json"
        with open(json_path, "w", encoding="utf-8") as out_fh:
            json.dump(parcel_json, out_fh, indent=2)

        print(f"✅ [{index}] Saved → {json_path}")
        results[index] = True

    except Exception as e:
        print(f"❌ [{index}] Failed for {orig_addr}: {e}")
        results[index] = False

def main():
    OUT_DIR.mkdir(exist_ok=True)
    DOCS_DIR.mkdir(exist_ok=True)

    with open(CSV_PATH, newline="", encoding="utf-8-sig") as fh:
        reader = list(csv.DictReader(fh))

    results = [False] * len(reader)

    with ThreadPoolExecutor(max_workers=1) as executor:
        futures = [executor.submit(process_row, idx, row, results)
                   for idx, row in enumerate(reader)
                   if row["county"].strip().lower() == "broward county"]
        for future in as_completed(futures):
            future.result()

    for i, row in enumerate(reader):
        row["extracted"] = str(results[i])

    with open(TRACKED_CSV_PATH, "w", newline="", encoding="utf-8") as out_csv:
        fieldnames = list(reader[0].keys())
        writer = csv.DictWriter(out_csv, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(reader)

    print(f"\n📄 Updated tracking CSV saved as → {TRACKED_CSV_PATH}")

if __name__ == "__main__":
    main()

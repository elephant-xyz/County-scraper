import requests
from bs4 import BeautifulSoup
import json
import os
import boto3
import backoff
import re
import time
import io
from tqdm import tqdm
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ProfileNotFound


class AccessDeniedRetryable(requests.exceptions.RequestException):
    pass

@backoff.on_exception(
    backoff.expo,
    (requests.exceptions.RequestException, AccessDeniedRetryable),
    max_tries=5,
    jitter=backoff.full_jitter
)
@backoff.on_predicate(
    backoff.expo,
    max_tries=5,
)
def scrape_full_parcel(url, folio_id=None):
    headers = {
        "User-Agent": "curl/7.79.1",  # mimic curl
        "Accept": "*/*",
    }
    response = requests.get(url, headers=headers)

    if "Access Denied" in response.text or "errors.edgesuite.net" in response.text:
        print(f"Access Denied for {url}, retrying...")
        raise AccessDeniedRetryable("Access Denied")

    html_content = response.content

    # save locally
    html_file_path = os.path.join("test_770", f"{folio_id}.html")
    with open(html_file_path, "wb") as f:
        f.write(html_content)


    return folio_id

def download_html_data(folio_ids, max_threads=10):
    base_url = "https://www.leepa.org/Display/DisplayParcel.aspx?FolioID={}&AuthDetails=True&PropertyDetailsCurrent=True&historyDetails=True&SalesDetails=True&PermitDetails=True&RenumberDetails=True&GarbageDetails=True&ElevationDetails=True&RPDetails=True"

    def download_and_store(folio_id):
        try:
            url = base_url.format(folio_id)
            result = scrape_full_parcel(url, folio_id)

            return result
        except Exception as e:
            if folio_id:
                error_text = f"Scrape failed for folio ID: {folio_id}\n\nError:\n{str(e)}"
                # Save failed HTML response to S3
                json_path = os.path.join("failed_data", f"{folio_id}.json")
                with open(json_path, "w") as f:
                    json.dump(error_text, f, indent=4)
                print(f"saved failed HTML for {folio_id}")

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(download_and_store, fid): fid for fid in folio_ids}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Scraping Progress"):
            future.result()


if __name__ == '__main__':

    start_time = time.time()
    df = pd.read_csv("../data/test.csv", dtype=str)
    df["FolioID"] = df["FolioID"].astype(str)
    df.set_index("FolioID", inplace=True)
    folio_ids = df.index.tolist()
    print(f"Loaded {len(folio_ids)} folio IDs from file.")
    download_html_data(folio_ids, max_threads=30)
    # Scrape data using threading
    # scrape_multiple_parcels(folio_ids, df, max_threads=20)
    end_time = time.time()
    duration = end_time - start_time
    print(f"All parcels scraped using threading.")
    print(f"Total time taken: {duration:.2f} seconds")

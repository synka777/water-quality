import requests
import json
from time import sleep
from pathlib import Path


BASE_URL = "https://hubeau.eaufrance.fr/api/v1/qualite_eau_potable/resultats_dis"

PARAMS = {
    "date_debut_prelevement": "2026-01-01",
    "date_fin_prelevement": "2026-12-31",
    "size": 10000,  # safe value
    "page": 1
}
BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_FILE = BASE_DIR / "data" / "water_quality_2026.json"
OUTPUT_FILE = str(OUTPUT_FILE)
MAX_RETRIES = 3


def fetch_all_data():
    page = 1
    total_fetched = 0

    with open(OUTPUT_FILE, "w") as f:
        while True:
            PARAMS["page"] = page
            print(f"Fetching page {page}...")

            retries = 0
            while retries < MAX_RETRIES:
                try:
                    response = requests.get(BASE_URL, params=PARAMS, timeout=30)

                    if response.status_code not in [200, 206]:
                        print(f"Error: {response.status_code}")
                        print(f"Response text: {response.text[:200]}")
                        return

                    data = response.json()

                    results = data.get("data", [])
                    if not results:
                        print("No more data.")
                        return

                    for row in results:
                        f.write(json.dumps(row) + "\n")

                    total_fetched += len(results)
                    print(f"Fetched {len(results)} rows (total: {total_fetched})")

                    # stop if last page
                    if not data.get("next"):
                        break

                    page += 1
                    sleep(0.5)  # be nice to API
                    break  # success, exit retry loop

                except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
                    retries += 1
                    if retries < MAX_RETRIES:
                        print(f"Request failed ({type(e).__name__}), retrying... (attempt {retries}/{MAX_RETRIES})")
                        sleep(2 ** retries)  # exponential backoff
                    else:
                        print(f"Request failed after {MAX_RETRIES} attempts. Stopping.")
                        return
            else:
                # If we exhausted retries and got to next page, but loop ended without break
                print("Failed to fetch page, stopping.")
                return

    print(f"\nDone. Total rows: {total_fetched}")


if __name__ == "__main__":
    fetch_all_data()
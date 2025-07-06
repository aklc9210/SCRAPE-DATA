from playwright.sync_api import sync_playwright
import pandas as pd
import re
import csv
from geopy.geocoders import Nominatim

def extract_store_data(full_text):
    match = re.match(r'^(.*?)\s*\((.*?)\)$', full_text)
    if match:
        store_name = match.group(1).strip()
        store_address = match.group(2).strip()
        return store_name, store_address
    
def parse_address(address: str) -> dict:
    parts = [p.strip() for p in address.split(",")]

    result = {
        "house_and_street": None,
        "hamlet": None,
        "ward": None,
        "district": None,
        "city": None
    }

    # at least house_number + street_name
    if len(parts) >= 2:
        result["house_and_street"] = f"{parts[0]}, {parts[1]}"

    # find known parts by keyword
    remaining = parts[2:]
    
    for part in remaining:
        if part.lower().startswith("ấp") or part.lower().startswith("thôn"):
            result["hamlet"] = part
        elif part.lower().startswith("xã") or part.lower().startswith("phường"):
            result["ward"] = part
        elif part.lower().startswith("huyện") or part.lower().startswith("quận"):
            result["district"] = part
        elif part.lower().startswith("thành phố") or part.lower().startswith("tp"):
            result["city"] = part

    return result


def main():
    geolocator = Nominatim(user_agent="bhx_scraper")
    page_url = "https://www.bachhoaxanh.com/he-thong-cua-hang"


    # Initialize geolocator
    geolocator = Nominatim(user_agent="bhx_scraper")

    rows = []
    with sync_playwright() as p:

        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        page.goto(page_url, timeout=60000)

        page.wait_for_selector("li.border-b-basic-400", timeout=15000)

        # keep clicking "Xem thêm" button until it's gone
        while True:
            try:
                load_more_btn = page.locator("//div[contains(@class, 'cursor-pointer')]//span[contains(text(), 'Xem thêm')]")

                if not load_more_btn.is_visible():
                    break
                load_more_btn.click()
                page.wait_for_timeout(5000)  # wait 1s for new stores to load
            except:
                break

        bhx_stores = page.locator("li.border-b-basic-400")
        bhx_stores_count = bhx_stores.count()
        print(f"Found {bhx_stores_count} stores")

        for i in range(bhx_stores_count):
            li = bhx_stores.nth(i)
            full_text = li.locator("span.mr-2.font-bold").inner_text()
            
            # Extracting store name and address
            store_name, store_address = extract_store_data(full_text)

            # Parsing the address into components
            comps = parse_address(store_address)

            # geocode (lat/lon)
            try:
                loc = geolocator.geocode(store_address, timeout=10)
                lat, lon = (loc.latitude, loc.longitude) if loc else (None, None)
            except Exception:
                lat = lon = None

            rows.append({
                "store_name":       store_name,
                "house_and_street": comps["house_and_street"],
                "hamlet":           comps["hamlet"],
                "ward":             comps["ward"],
                "district":         comps["district"],
                "city":             comps["city"],
                "lat":              lat,
                "lon":              lon
            })

        # build DataFrame
        df = pd.DataFrame(rows, columns=[
            "store_name",
            "house_and_street",
            "hamlet",
            "ward",
            "district",
            "city",
            "lat",
            "lon"
        ])

        df.to_csv("bhx_stores.csv", index=False, encoding="utf-8")
        browser.close()

    print(f"Data saved to bhx_stores.csv")

if __name__ == "__main__":
    main()
            
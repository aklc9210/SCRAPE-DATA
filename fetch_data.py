#!/usr/bin/env python3
"""
run_all_data.py

Orchestrates:
  1. fetch_full_location_data()  -- get list of provinces
  2. fetch_stores(...)           -- get stores for each province
Aggregates all store records into a single CSV.
"""

from fetch_full_location import fetch_full_location_data
from fetch_store_by_province import fetch_stores
import pandas as pd

def main():
    # 1) Get full location hierarchy (we only need provinces here)
    loc_data = fetch_full_location_data()
    provinces = loc_data.get("provinces", [])
    print(f"Found {len(provinces)} provinces.")

    all_records = []
    # 2) For each province, fetch its stores
    for prov in provinces:
        prov_id = prov.get("id")
        prov_name = prov.get("name", "")
        print(f"Fetching stores for province {prov_id} – {prov_name}...")
        stores = fetch_stores(province_id=prov_id, district_id=0, ward_id=0, page_size=100)
        for s in stores:
            # annotate with province info
            s["province_id"] = prov_id
            s["province_name"] = prov_name
        all_records.extend(stores)

    # 3) Normalize to DataFrame
    df = pd.json_normalize(all_records)
    df.rename(columns={
        "storeId": "store_id",
        "lat": "latitude",
        "lng": "longitude",
        "storeLocation": "store_location",
        "provinceId": "province_id",
        "districtId": "district_id",
        "wardId": "ward_id",
        "isStoreVirtual": "is_store_virtual",
        "openHour": "open_hour"
    }, inplace=True)

    # 4) Save to CSV
    output_path = "all_bhx_stores.csv"
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"All data saved to {output_path}")

if __name__ == "__main__":
    main()


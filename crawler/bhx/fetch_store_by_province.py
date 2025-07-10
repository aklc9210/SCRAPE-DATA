import requests
import pandas as pd
import time
import asyncio
from curl_cffi.requests import Session
from crawler.bhx.token_interceptor import BHXTokenInterceptor
from crawler.bhx.token_interceptor import get_headers

session_store = Session(impersonate="chrome110")
API_URL = "https://apibhx.tgdd.vn/Location/V2/GetStoresByLocation"

# fetch stores by province, district, ward
async def fetch_stores_async(province_id: int, token: str, deviceid: str, 
                           district_id: int = 0, ward_id: int = 0, page_size: int = 50):
    headers = get_headers(token, deviceid)
    stores = []
    page_index = 0

    while True:
        params = {
            "provinceId": province_id,
            "districtId": district_id,
            "wardId": ward_id,
            "pageSize": page_size,
            "pageIndex": page_index
        }
        
        try:
            resp = session_store.get(API_URL, headers=headers, params=params, timeout=15)
            
            if resp.status_code != 200:
                print(f"Failed to fetch stores for province {province_id}, page {page_index}, status = {resp.status_code}")
                break
                
            data = resp.json()
            batch = data.get("data", {}).get("stores", [])
            total = data.get("data", {}).get("total", 0)
            
            if not batch:
                print(f"No more stores found on page {page_index}")
                break

            # for store in stores:

                
            stores.extend(batch)
            print(f"Fetched {len(batch)} stores on page {page_index} for province {province_id}")
            page_index += 1
            
            if len(stores) >= total:
                print(f"All {total} stores fetched for province {province_id}")
                break
                
            await asyncio.sleep(0.3)
            
        except Exception as e:
            print(f"Error fetching stores for province {province_id}, page {page_index}: {e}")
            break

    return stores
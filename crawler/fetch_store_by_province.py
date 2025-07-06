import requests
import pandas as pd
import time
import asyncio
from curl_cffi.requests import Session
from token_interceptor import BHXTokenInterceptor

session_store = Session(impersonate="chrome110")
API_URL = "https://apibhx.tgdd.vn/Location/V2/GetStoresByLocation"

def get_store_headers(token, deviceid):
    """Generate headers for store fetching"""
    return {
        "Accept": "application/json, text/plain, */*",
        "Authorization": token,
        "xapikey": "bhx-api-core-2022",
        "platform": "webnew",
        "reversehost": "http://bhxapi.live",
        "origin": "https://www.bachhoaxanh.com",
        "referer": "https://www.bachhoaxanh.com/he-thong-cua-hang",
        "referer-url": "https://www.bachhoaxanh.com/he-thong-cua-hang",
        "content-type": "application/json",
        "deviceid": deviceid,
        "customer-id": "",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
    }

async def fetch_stores_async(province_id: int, token: str, deviceid: str, 
                           district_id: int = 0, ward_id: int = 0, page_size: int = 50):
    """
    Async version: Fetch all stores for the given province/district/ward
    """
    headers = get_store_headers(token, deviceid)
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
                
            stores.extend(batch)
            print(f"Fetched {len(batch)} stores on page {page_index} for province {province_id}")
            page_index += 1
            
            if len(stores) >= total:
                print(f"All {total} stores fetched for province {province_id}")
                break
                
            await asyncio.sleep(0.3)  # async sleep
            
        except Exception as e:
            print(f"Error fetching stores for province {province_id}, page {page_index}: {e}")
            break

    return stores

def fetch_stores(province_id: int, district_id: int = 0, ward_id: int = 0, page_size: int = 50):
    """
    Synchronous wrapper that gets token automatically
    """
    async def _fetch():
        # Get token automatically
        interceptor = BHXTokenInterceptor()
        token, deviceid = await interceptor.init_and_get_token()
        
        if not token:
            print("Failed to intercept token!")
            await interceptor.close()
            return []
            
        try:
            stores = await fetch_stores_async(province_id, token, deviceid, district_id, ward_id, page_size)
            return stores
        finally:
            await interceptor.close()
    
    return asyncio.run(_fetch())
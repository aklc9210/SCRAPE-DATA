import requests
import pandas as pd
import asyncio
from curl_cffi.requests import Session
from token_interceptor import BHXTokenInterceptor
from token_interceptor import get_headers

session = Session(impersonate="chrome110")
FULL_API_URL = "https://apibhx.tgdd.vn/Location/V2/GetFull"

# def get_headers(token, deviceid):
#     """Generate headers with intercepted token and deviceid"""
#     return {
#         "Accept": "application/json, text/plain, */*",
#         "Authorization": token,
#         "xapikey": "bhx-api-core-2022",
#         "platform": "webnew",
#         "reversehost": "http://bhxapi.live",
#         "origin": "https://www.bachhoaxanh.com",
#         "referer": "https://www.bachhoaxanh.com/he-thong-cua-hang",
#         "referer-url": "https://www.bachhoaxanh.com/he-thong-cua-hang",
#         "content-type": "application/json",
#         "deviceid": deviceid,
#         "customer-id": "",
#         "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
#     }

async def fetch_full_location_data():
    """
    Fetches the full hierarchical location data with auto token interception
    """
    # Get token automatically
    interceptor = BHXTokenInterceptor()
    token, deviceid = await interceptor.init_and_get_token()
    
    if not token:
        print("Failed to intercept token!")
        await interceptor.close()
        return {}
        
    headers = get_headers(token, deviceid)
    
    try:
        print("Fetching full location data...")
        resp = session.get(FULL_API_URL, headers=headers, timeout=15)
        print("Status code:", resp.status_code)
        
        if resp.status_code != 200:
            print("Response text:", resp.text[:500])
            resp.raise_for_status()
            
        payload = resp.json().get("data", {})
        print(f"Successfully fetched location data with {len(payload.get('provinces', []))} provinces")
        return payload
        
    except Exception as e:
        print("Request failed:", e)
        return {}
    finally:
        await interceptor.close()

# Synchronous wrapper for compatibility
def fetch_full_location_data_sync():
    """Synchronous wrapper for the async function"""
    return asyncio.run(fetch_full_location_data())
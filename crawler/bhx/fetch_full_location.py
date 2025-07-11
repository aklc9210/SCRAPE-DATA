import requests
import pandas as pd
import asyncio
from curl_cffi.requests import Session
from crawler.bhx.token_interceptor import BHXTokenInterceptor
from crawler.bhx.token_interceptor import get_headers

session = Session(impersonate="chrome110")
FULL_API_URL = "https://apibhx.tgdd.vn/Location/V2/GetFull"

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
    return asyncio.run(fetch_full_location_data())
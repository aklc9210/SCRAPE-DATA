import aiohttp
from typing import List, Dict
from crawler.winmart.config import API_BASE_V1, HEADERS

async def fetch_branches(province_codes: list = ["HCM"]) -> List[Dict]:
    """
    Fetch WinMart store branches by province codes.
    """
    url = f"{API_BASE_V1}/store-by-province"
    async with aiohttp.ClientSession(headers=HEADERS) as session:
        params = {'PageNumber': 1, 'PageSize': 1000, 'ProvinceCode': province_codes}
        async with session.get(url, params=params, timeout=10) as resp:
            if resp.status != 200:
                raise Exception(f"Failed to fetch branches, status: {resp.status}")
            data = await resp.json()
            stores = []
            for district in data.get("data", []):
                for ward in district.get("wardStores", []):
                    for store in ward.get("stores", []):
                        if store.get("provinceCode") in province_codes and store.get("activeStatus", "").strip() == "":
                            stores.append({
                                "code": store.get("storeCode"),
                                "name": store.get("storeName"),
                                "address": store.get("officeAddress"),
                                "provinceCode": store.get("provinceCode"),
                            })
            return stores

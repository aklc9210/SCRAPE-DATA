import aiohttp
from typing import List, Dict
from crawler.winmart.config import API_BASE_V1, HEADERS

class WinMartBranchFetcher:
    def __init__(self):
        self.url = f"{API_BASE_V1}/store-by-province"
        self.headers = HEADERS
        self.province_codes = ["HCM"]

    async def fetch_branches(self) -> List[Dict]:
        async with aiohttp.ClientSession(headers=self.headers) as session:
            params = {'PageNumber': 1, 'PageSize': 1000, 'ProvinceCode': self.province_codes}
            async with session.get(self.url, params=params, timeout=10) as resp:
                if resp.status != 200:
                    raise Exception("Failed")
                data = await resp.json()
        
                stores = []
                for district in data.get("data", []):
                    for ward in district.get("wardStores", []):
                        for store in ward.get("stores", []):
                            if store.get("provinceCode") in self.province_codes and store.get("activeStatus", "").strip() == "":
                                stores.append({
                                    "code": store.get("storeCode"),
                                    "name": store.get("storeName"),
                                    "address": store.get("officeAddress"),
                                    "provinceCode": store.get("provinceCode"),
                                })
                return stores

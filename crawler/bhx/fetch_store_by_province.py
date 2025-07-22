import aiohttp
from crawler.bhx.token_interceptor import get_headers

API_URL = "https://apibhx.tgdd.vn/Location/V2/GetStoresByLocation"

# fetch stores by province, district, ward
async def fetch_stores_async(province_id: int, token: str, deviceid: str, 
                           district_id: int = 0, ward_id: int = 0, page_size: int = 50):
    headers = get_headers(token, deviceid)
    stores = []
    page_index = 0

    async with aiohttp.ClientSession(headers=headers) as sess:
        while True:
            params = {
                "provinceId": province_id,
                "districtId": district_id,
                "wardId": ward_id,
                "pageSize": page_size,
                "pageIndex": page_index
            }
            async with sess.get(API_URL, params=params, timeout=15) as resp:
                if resp.status != 200:
                    break
                data = await resp.json()
                batch = data.get("data", {}).get("stores", [])
                total = data.get("data", {}).get("total", 0)
                if not batch:
                    break
                stores.extend(batch)
                if len(stores) >= total:
                    break
                page_index += 1
            # await asyncio.sleep(0.2)
    return stores
import aiohttp
from crawler.bhx.token_interceptor import get_headers

MENU_API_URL = "https://apibhx.tgdd.vn/Menu/GetMenuV2"

# Fetch menu and transform to categories
async def fetch_menus_for_store(province_id, ward_id, store_id, token: str, deviceid: str):
    headers = get_headers(token, deviceid)
    menus = []
    page_index = 0

    async with aiohttp.ClientSession(headers=headers) as sess:
        
        while True:
            params = {
                "ProvinceId": province_id,
                "WardId": ward_id,
                "StoreId": store_id
            }
            try:
                async with sess.get(MENU_API_URL, params=params, timeout=15) as resp:
                    if resp.status != 200:
                        # print(f"Failed menu fetch for store {store_id}: {resp.status}")
                        break
                    data = await resp.json()
                    batch = data.get("data", {}).get("menus", [])
                    total = data.get("data", {}).get("totalPromotions", 0)

                    if not batch:
                        # print(f"No menu found for store {store_id}")
                        break
                    
                    menus.extend(batch)
                    
                    # print(f"Fetched {len(batch)} items for store {store_id} on page {page_index + 1}")
                    page_index += 1
                    
                    if len(menus) >= total:
                        # print(f"All menu items fetched for store {store_id}")
                        break

                    # await asyncio.sleep(0.3)
            except Exception as e:
                # print(f"Error fetching menu for store {store_id}: {e}") 
                break
            
        return menus
import asyncio
import aiohttp
from typing import List
from curl_cffi.requests import Session
from crawler.bhx.token_interceptor import BHXTokenInterceptor, get_headers
from crawler.bhx.fetch_store_by_province import fetch_stores_async
from crawler.bhx.fetch_full_location import FULL_API_URL
from crawler.bhx.fetch_menus_for_store import fetch_menu_for_store
from db import MongoDB
from crawler.bhx.process_data import (
    VALID_TITLES, CATEGORIES_MAPPING, 
    process_product_data, upsert_products_bulk,
    parse_store_line
)
from pymongo import UpdateOne

session = Session(impersonate="chrome110")
db = MongoDB.get_db()

class BHXDataFetcher:
    def __init__(self):
        self.token = None
        self.deviceid = None
        self.interceptor = None
        self.session = None  # Add aiohttp session
        
        # Upsert chains to database
        self._init_chains()
    
    def _init_chains(self):
        """Initialize chain data in database"""
        chain_coll = db.chains
        chains = [
            {"code": "BHX", "name": "Bách Hóa Xanh"},
            {"code": "WM", "name": "Winmart"}
        ]
        for chain in chains:
            chain_coll.update_one(
                {"code": chain["code"]},
                {"$set": chain},
                upsert=True
            )
            print(f"✓ Upserted chain: {chain['name']}")
    
    async def init_token(self):
        """Initialize BHX token for API calls"""
        print("Initializing token interception...")
        self.interceptor = BHXTokenInterceptor()
        self.token, self.deviceid = await self.interceptor.init_and_get_token()
        
        if not self.token:
            raise Exception("Failed to intercept token. Please check your internet connection.")
        
        # Initialize aiohttp session
        self.session = aiohttp.ClientSession()
        print(f"Token intercepted successfully!")
        return True
    
    async def fetch_products_direct_api(self, store_id: int, category_url: str, 
                                       province_id: int = 3, district_id: int = 0, ward_id: int = 0) -> List[dict]:
        """
        🔥 GỌI TRỰC TIẾP BHX API cho từng store
        """
        all_products = []
        page_size = 20
        page = 1
        
        print(f"🔥 Direct API call for store {store_id}, category {category_url}")
        
        while True:
            # Build API URL với storeId cụ thể
            api_url = (
                f"https://apibhx.tgdd.vn/Category/V2/GetCate?"
                f"provinceId={province_id}&wardId={ward_id}&districtId={district_id}"
                f"&storeId={store_id}&categoryUrl={category_url}"
                f"&isMobile=true&isV2=true&pageSize={page_size}&page={page}"
            )
            
            # Headers với token
            headers = get_headers(self.token, self.deviceid)
            headers.update({
                "referer": f"https://www.bachhoaxanh.com/{category_url}",
                "accept": "application/json, text/plain, */*"
            })
            
            try:
                async with self.session.get(api_url, headers=headers, timeout=10) as response:
                    if response.status != 200:
                        print(f"❌ API error {response.status} for store {store_id}")
                        break
                    
                    data = await response.json()
                    products_data = data.get("data", {})
                    products_batch = products_data.get("products", [])
                    total = products_data.get("total", 0)
                    
                    if not products_batch:
                        print(f"✅ No more products for store {store_id} (page {page})")
                        break
                    
                    all_products.extend(products_batch)
                    print(f"📦 Store {store_id}: +{len(products_batch)} products (total: {len(all_products)}/{total})")
                    
                    # Check if we got all products
                    if len(all_products) >= total:
                        print(f"🎉 Got all {total} products for store {store_id}")
                        break
                    
                    # Next page
                    page += 1
                    await asyncio.sleep(0.5)  # Rate limiting
                    
            except Exception as e:
                print(f"❌ Error fetching store {store_id}, page {page}: {e}")
                break
        
        return all_products

    async def _fetch_and_upsert_categories(self):
        """Fetch menu categories and upsert to database"""
        categories = []
        menus = await fetch_menu_for_store(3, 2087, 4946, self.token, self.deviceid)
        
        for menu in menus:
            print(f"Danh mục cha: {menu['name']}")
            
            for child in menu.get("childrens", []):
                category = CATEGORIES_MAPPING.get(child['name'])

                if child['name'] in VALID_TITLES:
                    categories.append({
                        "name": category,
                        "link": child['url']
                    })
                    print(f"Category: {category} - {child['name']}")
                else:
                    print(f"{child['name']} (ID: {child['id']}) - bỏ qua")
        
        # Upsert categories to database
        if categories:
            category_db = db.categories
            grouped = {}
            for cat in categories:
                name = cat.get('name')
                link = cat.get('link')
                if name and link:
                    grouped.setdefault(name, []).append(link)

            for name, links in grouped.items():
                unique_links = list(dict.fromkeys(links))
                category_db.update_one(
                    {"name": name},
                    {"$set": {"links": unique_links}},
                    upsert=True
                )
            print(f"✓ Upserted {len(grouped)} distinct categories to MongoDB.")
        
        return categories
    
    async def _fetch_and_upsert_provinces(self):
        """Fetch province data and upsert to database"""
        headers = get_headers(self.token, self.deviceid)
        
        try:
            resp = session.get(FULL_API_URL, headers=headers, timeout=15)
            if resp.status_code != 200:
                raise Exception(f"Failed to fetch provinces: {resp.status_code}")
            
            loc_data = resp.json().get("data", {})
            provinces = loc_data.get("provinces", [])

            # Filter for TPHCM only
            for province in provinces:
                if province.get('name', "").strip() == "TP. Hồ Chí Minh":
                    provinces = [province]
                    break

            print(f"Found {len(provinces)} provinces.")
            
            # Upsert provinces to database
            self._upsert_provinces_to_db(provinces)
            return provinces
            
        except Exception as e:
            print(f"Error fetching provinces: {e}")
            return []
    
    def _upsert_provinces_to_db(self, provinces):
        """Upsert province data to database"""
        try:
            prov_db = db.provinces
            for prov in provinces:
                prov_id = prov.get("id")
                prov_name = prov.get("name", "")
                prov_district = prov.get("districts", [])
                
                prov_db.update_one(
                    {"_id": prov_id},
                    {"$set": {"name": prov_name, "district": prov_district}},
                    upsert=True
                )
                print(f"✓ Upserted province: {prov_name} (ID: {prov_id})")
        except Exception as e:
            print(f"Error upserting provinces: {e}")
    
    def _upsert_stores_to_db(self, stores_data):
        """Upsert store data to database"""
        if not stores_data:
            print("No store data to save.")
            return
            
        print(f"Saving {len(stores_data)} stores to db...")
        store_db = db.stores
        
        for store in stores_data:
            store_id = store.get("storeId")
            if not store_id:
                continue

            store_title = parse_store_line(store.get("storeLocation", ""))
            
            store_data = {
                "store_id": store_id,
                "store_name": store_title['store_name'],
                "latitude": store.get("lat", 0.0),
                "longitude": store.get("lng", 0.0),
                "store_location": store_title['store_location'],
                "province_id": store.get("provinceId", 0),
                "province": store.get("province", ""),
                "district_id": store.get("districtId", 0),
                "district": store.get("district", ""),
                "ward_id": store.get("wardId", 0),
                "ward": store.get("ward", ""),
                "is_store_virtual": store.get("isStoreVirtual", False),
                "open_hour": store.get("openHour", ""),
                "phone_number": store.get("phone", ""),
                "store_status": store.get("status", ""),
                "chain": "BHX"
            }
            
            store_db.update_one(
                {"store_id": store_id},
                {"$set": store_data},
                upsert=True
            )

        print(f"✓ Upserted {len(stores_data)} stores to MongoDB.")
    
    async def upsert_products_bulk_local(self, product_list: List[dict], category_title: str):
        """🔥 BULK UPSERT products to MongoDB by category"""
        coll_name = category_title.replace(" ", "_").replace("&", "and").replace(":", "").lower()
        collection = db[coll_name]
        operations = []
        
        for p in product_list:
            sku = p.get("sku")
            if not sku:
                continue
            operations.append(
                UpdateOne(
                    {"sku": sku, "store_id": p.get("store_id")},
                    {"$set": p},
                    upsert=True
                )
            )
        
        if not operations:
            print(f"⚠️ No valid products to save for {category_title}")
            return
        
        result = collection.bulk_write(operations, ordered=False)
        print(f"✅ Bulk saved {len(operations)} products to `{coll_name}` "
              f"(upserted: {result.upserted_count}, modified: {result.modified_count})")
    
    async def fetch_all_stores_direct_api(self):
        """
        🔥 MAIN METHOD: Crawl tất cả stores bằng direct API calls
        """
        print('=== Starting BHX Direct API Crawling ===')
        
        # Initialize token and session
        await self.init_token()
        
        # Setup categories and provinces
        await self._fetch_and_upsert_categories()
        provinces = await self._fetch_and_upsert_provinces()
        
        if not provinces:
            print("No provinces found.")
            return []
        
        # Get stores
        all_records = []
        for prov in provinces:
            prov_id = prov.get("id")
            prov_name = prov.get("name", "")
            
            # Fetch stores for province
            stores = await fetch_stores_async(
                province_id=prov_id,
                token=self.token,
                deviceid=self.deviceid,
                district_id=0,
                ward_id=0,
                page_size=100
            )
            
            # Limit for testing
            stores = stores[50:51] if len(stores) > 3 else stores
            print(f"📍 Processing {len(stores)} stores in {prov_name}")
            
            # Save stores to DB
            self._upsert_stores_to_db(stores)
            
            # CRAWL PRODUCTS FOR EACH STORE using Direct API
            for store in stores:
                store_id = store.get('storeId')
                ward_id = store.get('wardId', 0)
                district_id = store.get('districtId', 0)
                
                print(f"\n🏪 Processing store {store_id}")
                
                # Crawl each category for this store
                for cat_doc in db.categories.find({}):
                    category_name = cat_doc['name']
                    category_links = cat_doc.get('links', [])
                    
                    all_products_for_category = []
                    
                    for category_url in category_links:
                        # 🔥 DIRECT API CALL với storeId cụ thể
                        raw_products = await self.fetch_products_direct_api(
                            store_id=store_id,
                            category_url=category_url,
                            province_id=prov_id,
                            district_id=district_id,
                            ward_id=ward_id
                        )
                        
                        # Process products
                        for product in raw_products:
                            try:
                                processed = process_product_data(
                                    product=product,
                                    category_name=category_name,
                                    store_id=store_id
                                )
                                
                                # Chỉ add nếu không bị skip
                                if processed is not None:
                                    all_products_for_category.append(processed)
                                # Nếu processed = None → skip, không add vào list
                                
                            except Exception as e:
                                print(f"Error processing product: {e}")
                                continue
                    
                    # Bulk save products for this category and store
                    if all_products_for_category:
                        await self.upsert_products_bulk_local(all_products_for_category, category_name)
                        print(f"💾 Saved {len(all_products_for_category)} NEW products "
                            f"({category_name}) for store {store_id}")
                    else:
                        print(f"⏭️ All products already exist for {category_name} in store {store_id}")
            
            all_records.extend(stores)
        
        print(f"\n🎉 Crawling completed! Total stores: {len(all_records)}")
        return all_records
    
    async def close(self):
        """Clean up resources"""
        if self.session:
            await self.session.close()
        if self.interceptor:
            await self.interceptor.close()


async def main():
    """Main entry point"""
    fetcher = BHXDataFetcher()
    
    try:
        # 🔥 Use direct API method instead of Playwright
        stores_data = await fetcher.fetch_all_stores_direct_api()
        
        if stores_data:
            print(f"\n=== SUMMARY ===")
            print(f"✅ Total stores processed: {len(stores_data)}")
        else:
            print("❌ No stores processed.")
            
    except Exception as e:
        print(f"💥 Error in main: {e}")
    finally:
        await fetcher.close()


def run_sync():
    asyncio.run(main())


if __name__ == "__main__":
    run_sync()
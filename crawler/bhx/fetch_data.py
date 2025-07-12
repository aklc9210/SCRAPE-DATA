import asyncio
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
    parse_store_line, reset_category_collections
)
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

session = Session(impersonate="chrome110")
db = MongoDB.get_db()

async def fetch_category_products(url: str, step: int = 3, timeout: float = 8.0):  
    """
    Thu thập sản phẩm BHX bằng cách intercept AjaxProduct API
    Dựa trên phân tích thực tế: API POST https://apibhx.tgdd.vn/Category/AjaxProduct
    """
    products, seen = [], set()
    total = None
    consecutive_failures = 0
    
    print(f"🚀 Bắt đầu crawl: {url}")

    async def on_response(resp):
        nonlocal total, consecutive_failures
        
        if "AjaxProduct" in resp.url and resp.status == 200:
            try:
                js = await resp.json()
                data = js.get("data", {})
                
                if "total" in data:
                    total = data["total"]
                    print(f"📊 Tổng sản phẩm có sẵn: {total}")
                
                batch = data.get("products", [])
                new_count = 0
                
                for p in batch:
                    pid = p.get("id")
                    if pid and pid not in seen:
                        seen.add(pid)
                        products.append(p)
                        new_count += 1
                
                if new_count > 0:
                    print(f"✅ +{new_count} sản phẩm mới (tổng: {len(products)}/{total or '?'})")
                    consecutive_failures = 0
                else:
                    print(f"⚠️ Không có sản phẩm mới từ API response")
                    consecutive_failures += 1
                    
            except Exception as e:
                print(f"❌ Lỗi parse JSON từ AjaxProduct: {e}")
                consecutive_failures += 1

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-dev-shm-usage']
        )
        
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        )
        
        page = await context.new_page()
        page.on("response", on_response)

        # Load trang ban đầu
        print("🔄 Đang load trang...")
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=10000)
            print("✅ Trang đã load thành công")
            await asyncio.sleep(3)
            
        except Exception as e:
            print(f"❌ Load trang thất bại: {e}")
            await browser.close()
            return []
        
        # Scroll strategy: batch scroll để trigger API calls
        scroll_count = 0
        max_scrolls = 50
        
        print("🔄 Bắt đầu scroll theo pattern 2-3 scroll/API call...")
        
        while scroll_count < max_scrolls:
            # Scroll 2-3 lần liên tiếp để trigger API
            batch_scrolls = min(3, max_scrolls - scroll_count)
            
            print(f"🔄 Batch scroll #{scroll_count//3 + 1} (scrolls {scroll_count+1}-{scroll_count+batch_scrolls})")
            
            for i in range(batch_scrolls):
                scroll_count += 1
                
                await page.evaluate("""
                    () => {
                        const scrollDistance = window.innerHeight * 0.8;
                        window.scrollBy(0, scrollDistance);
                    }
                """)
                
                # Delay ngắn giữa các scroll trong batch
                await asyncio.sleep(0.8)
            
            # Sau khi scroll batch, chờ API response với timeout dài hơn
            print(f"📡 Chờ AjaxProduct response sau batch scroll...")
            
            try:
                await asyncio.wait_for(
                    page.wait_for_event(
                        "response", 
                        predicate=lambda r: "AjaxProduct" in r.url and r.status == 200
                    ),
                    timeout=12.0
                )
                print(f"✅ Nhận được AjaxProduct response")
                consecutive_failures = 0
                
            except asyncio.TimeoutError:
                print(f"⏰ Timeout sau batch scroll - có thể đã hết sản phẩm")
                consecutive_failures += 1
            
            # Kiểm tra điều kiện dừng
            if total and len(products) >= total:
                print(f"🎉 Đã crawl đủ {total} sản phẩm!")
                break
                
            if consecutive_failures >= 2:
                print(f"⚠️ Dừng sau {consecutive_failures} batch không có response")
                break
            
            # Delay giữa các batch để không spam
            await asyncio.sleep(1.5)

        await browser.close()
        
        print(f"✅ Hoàn thành! Thu thập được {len(products)} sản phẩm")
        print(f"📊 Chi tiết: {scroll_count} lần scroll, {consecutive_failures} lần thất bại cuối")
        
        return products


class BHXDataFetcher:
    def __init__(self):
        self.token = None
        self.deviceid = None
        self.interceptor = None
        
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
        
        print(f"Token intercepted successfully!")
        return True
    
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
    
    async def fetch_product_info(self, store_id: int, category_url: str):
        """Fetch products for a specific store and category"""
        full_url = f"https://www.bachhoaxanh.com/{category_url}"
        print(f"→ Scrolling to load all products from {full_url}")
        
        # Get raw products from website
        raw_products = await fetch_category_products(full_url, step=2, timeout=4.0)
        print(f"✓ Gathered {len(raw_products)} products after scrolling.")
        
        try:
            # Get category info from database
            cat = db.categories.find_one({"links": category_url})
            if not cat:
                print(f"Category not found for URL: {category_url}")
                return []
            
            # Process each product
            product_records = []
            for product in raw_products:
                try:
                    product_data = process_product_data(
                        product=product,
                        category_name=cat["name"],
                        store_id=store_id
                    )
                    product_records.append(product_data)
                except ValueError as e:
                    print(f"Error processing product: {e}")
                    continue

            return product_records
        
        except Exception as e:
            print(f"Error fetching products: {e}")
            return []
    
    async def fetch_all_stores(self):
        """Main method to fetch all stores and their products"""
        print('=== Starting BHX Stores Data Fetching ===')
        
        # Initialize token
        await self.init_token()
        
        # Fetch and setup categories
        await self._fetch_and_upsert_categories()
        
        # Fetch and setup provinces
        provinces = await self._fetch_and_upsert_provinces()
        if not provinces:
            print("No provinces found. Something went wrong.")
            return []

        # Create mapping dictionaries
        district_map = {}
        ward_map = {}
        for prov in provinces:
            for dist in prov.get("districts", []):
                district_map[dist["id"]] = dist["name"]
                for ward in dist.get("wards", []):
                    ward_map[ward["id"]] = ward["name"]
        
        all_records = []
        
        # Process each province
        for i, prov in enumerate(provinces, 1):
            prov_id = prov.get("id")
            prov_name = prov.get("name", "")
            print(f"\n[{i}/{len(provinces)}] Fetching stores for {prov_name} (ID: {prov_id})...")
            
            try:
                # Fetch stores in province
                stores = await fetch_stores_async(
                    province_id=prov_id,
                    token=self.token,
                    deviceid=self.deviceid,
                    district_id=0,
                    ward_id=0,
                    page_size=100
                )

                stores = stores[10:20] if len(stores) > 20 else stores

                # Add location info to all stores
                for store in stores:
                    ward_id = store.get("wardId", 0)
                    district_id = store.get("districtId", 0)
                    
                    # Add location info to store
                    store["province_id"] = prov_id
                    store["province"] = prov_name
                    store["district_id"] = district_id
                    store["district"] = district_map.get(district_id, "")
                    store["ward_id"] = ward_id
                    store["ward"] = ward_map.get(ward_id, "")
                
                # ✅ SAVE STORES TO DATABASE
                self._upsert_stores_to_db(stores)
                print(f"✓ Saved {len(stores)} stores to database for {prov_name}")

                # Process products for each store
                for store in stores:
                    store_id = store.get('storeId')
                    
                    # Fetch products for each category
                    all_products = []
                    for cat_doc in db.categories.find({}):
                        for category_url in cat_doc.get("links", []):
                            products = await self.fetch_product_info(store_id, category_url)
                            all_products.extend(products)
                        
                        # Bulk upsert products for this category
                        if all_products:
                            await upsert_products_bulk(all_products, cat_doc['name'], db)
                            all_products.clear()
                
                all_records.extend(stores)
                print(f"✓ Found {len(stores)} stores in {prov_name}")
                
            except Exception as e:
                print(f"✗ Error fetching stores for {prov_name}: {e}")
                continue

        print(f"\n=== Fetching completed! Total stores: {len(all_records)} ===")
        return all_records
    
    async def close(self):
        """Clean up resources"""
        if self.interceptor:
            await self.interceptor.close()


async def main():
    """Main entry point"""
    fetcher = BHXDataFetcher()
    
    try:
        stores_data = await fetcher.fetch_all_stores()
        
        if stores_data:
            print(f"\n=== STORE SUMMARY ===")
            print(f"Total stores: {len(stores_data)}")
            
            # Show stores count by province
            province_counts = {}
            for store in stores_data:
                prov = store.get('province', 'Unknown')
                province_counts[prov] = province_counts.get(prov, 0) + 1
            
            print(f"Provinces covered: {len(province_counts)}")
            print("\nStores by province:")
            sorted_provinces = sorted(province_counts.items(), key=lambda x: x[1], reverse=True)
            for prov, count in sorted_provinces:
                print(f"  {prov}: {count} stores")
                
        else:
            print("Failed to fetch stores data.")
            
    except Exception as e:
        print(f"Error in main: {e}")
    finally:
        await fetcher.close()


def run_sync():
    asyncio.run(main())


if __name__ == "__main__":
    run_sync()
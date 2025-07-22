import asyncio
import time
import aiohttp
from crawler.bhx.token_interceptor import BHXTokenInterceptor, get_headers
from crawler.bhx.fetch_store_by_province import fetch_stores_async
from crawler.bhx.fetch_full_location import fetch_full_location_data
from crawler.bhx.fetch_menus_for_store import fetch_menus_for_store
from db.db_async import get_db
from crawler.bhx.process_data import (
    CATEGORIES_MAPPING, 
    process_product_data,
)
from tqdm import tqdm
from asyncio import Semaphore


class BHXDataFetcher:
    def __init__(self, concurrency: int = 5):
        self.token = None
        self.deviceid = None
        self.interceptor = None
        self.session = None
        self.db = get_db()

        # semaphore để giới hạn số store chạy song song
        self.sem = Semaphore(concurrency)
        
        # Upsert chains to database
        # self._init_chains()
    
    async def init(self):
        ti = BHXTokenInterceptor()
        self.token, self.deviceid = await ti.init_and_get_token()
        await ti.close()
        self.session = aiohttp.ClientSession()

    async def close(self):
        await self.session.close()

    async def close(self):
        await self.session.close()

    async def fetch_categories(self, province, ward, store):
        raw = await fetch_menus_for_store(province, ward, store,
                                          self.token, self.deviceid)
        cats = []
        for m in raw:
            for c in m.get("childrens", []):
                eng = CATEGORIES_MAPPING.get(c["name"])
                if eng:
                    cats.append({"name":eng, "link":c["url"]})
        # group by name
        grouped = {}
        for c in cats:
            grouped.setdefault(c["name"], []).append(c["link"])
        return [{"name":n, "links":list(set(ls))} for n,ls in grouped.items()]

    async def crawl_store(self, store, categories, province):
        store_id = store["storeId"]
        ward_id  = store.get("wardId",0)
        dist_id  = store.get("districtId",0)

        for cat in tqdm(categories, desc=f"Store {store_id}", leave=False):
            for url in cat["links"]:
                await self.sem_wrap(
                    self.fetch_api_and_save(store_id, ward_id, dist_id, province, cat, url)
                )

    async def fetch_api_and_save(self, store_id, ward, dist, prov, cat, url):
        # fetch all pages
        allp = []
        page=1; size=20
        with tqdm(desc=f"Store {store_id} API", unit="page") as pbar:
            while True:
                api = (f"https://apibhx.tgdd.vn/Category/V2/GetCate?"
                    f"provinceId={prov}&wardId={ward}&districtId={dist}"
                    f"&storeId={store_id}&categoryUrl={url}"
                    f"&isMobile=true&pageSize={size}&page={page}")
                h = get_headers(self.token, self.deviceid)

                # Headers với token
                h.update({
                    "referer": f"https://www.bachhoaxanh.com/{cat}",
                    "accept": "application/json, text/plain, */*"
                })

                try:
                    async with self.session.get(api, headers=h, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                        js = await resp.json()
                except asyncio.TimeoutError:
                    print(f"⏱️ Timeout at page {page} for store {store_id}")
                    break
                except aiohttp.ClientError as e:
                    print(f"🌐 Network error for store {store_id}: {e}")
                    break
                except Exception as e:
                    print(f"💥 Unexpected error while fetching store {store_id}: {e}")
                    break

                batch = js["data"].get("products", [])
                total = js["data"].get("total", 0)

                if not batch: break

                allp.extend(batch)   
                if len(allp) >= total: 
                    break

                page+=1
                pbar.update(1)
                await asyncio.sleep(0.5)

        # build and write ops
        ops = await process_product_data(allp, cat["name"], store_id, self.db)
        if ops:
            coll = self.db[cat["name"].replace(" ","_").lower()]
            result = await coll.bulk_write(ops, ordered=False)
            print(f"Store {store_id}｜{cat['name']}: upserted {result.upserted_count}, "
                  f"mod {result.modified_count}")

    async def sem_wrap(self, coro):
        async with self.sem:
            try:
                return await coro
            except asyncio.TimeoutError:
                print("❌ TimeoutError while executing a task")
            except aiohttp.ClientError as e:
                print(f"❌ Network error: {e}")
            except Exception as e:
                print(f"❌ Unexpected error: {e}")
        

async def main():
    fetcher = BHXDataFetcher(concurrency=4)
    await fetcher.init()
    start = None
    end = None
    try:
        # 1. Categories from any sample store
        prov, ward, store0 = 3, 4946, 2087
        categories = await fetcher.fetch_categories(prov, ward, store0)

        # 2. Get stores in HCM
        full = await fetch_full_location_data(fetcher.token, fetcher.deviceid)
        provinces = [p for p in full.get("provinces",[]) if p["name"].strip() == "TP. Hồ Chí Minh"]
        if not provinces:
            print("No provinces"); return
        stores = await fetch_stores_async(provinces[0]["id"],
                                        fetcher.token, fetcher.deviceid)
        
        # test thử 1 store
        stores = stores[450:500]

        # 3. Crawl product
        start = time.time()
        await asyncio.gather(*[fetcher.sem_wrap(fetcher.crawl_store(s, categories, provinces[0]["id"]))
                            for s in stores])

        end = time.time()
        print(f"Total time: {(end - start):.2f} minutes")
    finally:
        await fetcher.close()

def run_sync():
    asyncio.run(main())

import asyncio
import time
import aiohttp
from typing import List
from curl_cffi.requests import Session
from crawler.bhx.token_interceptor import BHXTokenInterceptor, get_headers
from crawler.bhx.fetch_store_by_province import fetch_stores_async
from crawler.bhx.fetch_full_location import FULL_API_URL, fetch_full_location_data
from crawler.bhx.fetch_menus_for_store import fetch_menus_for_store
from db.db_async import db
from crawler.bhx.process_data import (
    CATEGORIES_MAPPING, 
    process_product_data,
    parse_store_line
)
from pymongo import UpdateOne
from tqdm import tqdm
from asyncio import Semaphore


class BHXDataFetcher:
    def __init__(self, concurrency: int = 5):
        self.token = None
        self.deviceid = None
        self.interceptor = None
        self.session = None

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

    async def crawl_store(self, store, categories, province, only_one_product=False):
        store_id = store["storeId"]
        ward_id  = store.get("wardId",0)
        dist_id  = store.get("districtId",0)

        for cat in tqdm(categories, desc=f"Store {store_id}", leave=False):
            for url in cat["links"]:
                await self.sem_wrap(
                    self.fetch_api_and_save(store_id, ward_id, dist_id, province, cat, url, only_one_product)
                )

    async def fetch_api_and_save(self, store_id, ward, dist, prov, cat, url, only_one_product=False):
        # fetch all pages
        allp = []
        page=1; size=50
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

            async with self.session.get(api, headers=h) as resp:
                js = await resp.json()
            batch = js["data"].get("products", [])
            if not batch: break

            # test 1 sp
            if only_one_product:
                allp.append(batch[0])
                break

            allp += batch
            if len(allp)>=js["data"].get("total",0): break
            page+=1
            # await asyncio.sleep(0.1)

        # build and write ops
        ops = await process_product_data(allp, cat["name"], store_id, db)
        if ops:
            coll = db[cat["name"].replace(" ","_").lower()]
            result = await coll.bulk_write(ops, ordered=False)
            print(f"Store {store_id}｜{cat['name']}: upserted {result.upserted_count}, "
                  f"mod {result.modified_count}")

    async def sem_wrap(self, coro):
        async with self.sem:
            return await coro

    
    def _init_chains(self):
        """Initialize chain data in database"""
        chain_coll = db.chains
        chains = [
            {"code": "BHX", "name": "Bách Hóa Xanh"},
            {"code": "WM", "name": "Winmart"}
        ]
        with tqdm(chains, desc="Initializing chains") as pbar:
            for chain in pbar:
                chain_coll.update_one(
                    {"code": chain["code"]},
                    {"$set": chain},
                    upsert=True
                )
                pbar.set_postfix_str(f"Upserted: {chain['name']}")

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
        stores = stores[191:192]

        # 3. Crawl product
        start = time.time()
        await asyncio.gather(*[fetcher.sem_wrap(fetcher.crawl_store(s, categories, provinces[0]["id"], only_one_product=False))
                            for s in stores])

        end = time.time()
        print(f"Total time: {(end - start):.2f} minutes")
    finally:
        await fetcher.close()

def run_sync():
    asyncio.run(main())

# orchestrator.py

import asyncio
from pymongo import UpdateOne
from tqdm.asyncio import tqdm
from crawler.winmart.fetch_branches import WinMartBranchFetcher
from crawler.winmart.fetch_product import WinMartProductFetcher
from crawler.winmart.data_processor import DataProcessor
from db.db_async import db

class WinMartFetcher:
    def __init__(self, concurrency: int = 4):
        self.branches = None
        self.fetcher = None
        self.processor = None
        self.db = None
        self.sem = None
        self.sem = asyncio.Semaphore(concurrency)   

    async def init(self):
        self.branches = await WinMartBranchFetcher().fetch_branches()
        self.fetcher = WinMartProductFetcher()
        await self.fetcher.init()

        self.processor = DataProcessor()
        self.db = db

    async def sem_wrap(self, coro, *args):
        async with self.sem:
            return await coro(*args)

    async def crawl_store(self, store: dict):
        sid = store.get("code")
        print(f"▶ Crawling store {sid}")

        # Fetch raw products for this store
        raws = await self.fetcher.fetch_products_by_store(sid)
        print(f"Retrieved {len(raws)} raw items from store {sid}")

        # Process raw items into normalized records
        records = await self.processor.process_products_batch(raws)
        if not records:
            print(f"No valid records for store {sid}, skipping.")
            return

        loop = asyncio.get_running_loop()
        total_upserted = 0
        total_modified = 0

        # Build category groups
        category_groups = {}
        for rec in records:
            coll = rec.get("category").replace(" ", "_").lower()
            category_groups.setdefault(coll, []).append(rec)

        # Bulk upsert per collection
        for coll_name, recs in category_groups.items():
            operations = [
                UpdateOne(
                    {"sku": r["sku"], "store_id": r["store_id"]},
                    {"$set": r},
                    upsert=True
                ) for r in recs
            ]
            if not operations:
                continue

            # Execute in threadpool
            result = await loop.run_in_executor(
                None,
                lambda ops=operations, col=coll_name: self.db[col].bulk_write(ops, ordered=False)
            )
            up = result.upserted_count
            md = result.modified_count
            total_upserted += up
            total_modified += md
            print(f"   • {coll_name}: upserted {up}, modified {md}")

        print(
            f"   ✔ Store {sid} summary: total upserted={total_upserted}, total modified={total_modified}"
        )

    async def run(self):
        await self.init()
        self.branches = self.branches[106:107]

        tasks = [self.sem_wrap(self.crawl_store, store) for store in self.branches]

        for task in tqdm(
            asyncio.as_completed(tasks),
            total=len(tasks),
            desc="Stores",
            unit="store"
        ):
            await task

        print("✅ Done.")



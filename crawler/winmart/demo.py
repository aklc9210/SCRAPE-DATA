import asyncio
import time
import functools
from pymongo import UpdateOne
from tqdm.asyncio import tqdm
from crawler.winmart.fetch_branches import fetch_branches
from crawler.winmart.fetch_category import fetch_categories
from crawler.winmart.fetch_product import fetch_products_by_store
from crawler.winmart.data_processor import process_products_batch
from db.db_async import get_db

class WinMartFetcher:
    def __init__(self, concurrency: int = 10):
        self.branches = None
        self.categories = None
        self.db = None
        self.sem = asyncio.Semaphore(concurrency)

    async def init(self):
        self.branches = await fetch_branches()
        self.categories = await fetch_categories()
        self.db = get_db()

    async def sem_wrap(self, coro, *args):
        async with self.sem:
            try:
                return await coro(*args)
            except asyncio.TimeoutError:
                print(f"❌ TimeoutError: {args[0] if args else 'Unknown'}")
            except Exception as e:
                print(f"❌ Exception while processing {args[0] if args else 'Unknown'}: {e}")


    async def crawl_store(self, store: dict):
        sid = store.get("code")
        print(f"▶ Crawling store {sid}")

        # Fetch raw products for this store
        raws = await fetch_products_by_store(sid, self.categories)

        # Process raw items into normalized records
        records = await process_products_batch(raws, self.db)
        if not records:
            print(f"No valid records for store {sid}, skipping.")
            return

        loop = asyncio.get_running_loop()
        # Build category groups
        category_groups = {}
        for rec in records:
            coll = rec.get("category", "general").replace(" ", "_").lower()
            category_groups.setdefault(coll, []).append(rec)

        # Bulk upsert per collection
        for coll_name, recs in tqdm(
            category_groups.items(),
            desc=f"  Store {sid}",
            unit="category",
            leave=False
        ):
            operations = [
                UpdateOne(
                    {"sku": r["sku"], "store_id": r["store_id"]},
                    {"$set": r},
                    upsert=True
                ) for r in recs
            ]
            if not operations:
                continue

            bulk_result = await self.db[coll_name].bulk_write(
                operations,
                ordered=False
            )
            print(f"[{coll_name}] upserted: {bulk_result.upserted_count}")

    async def run(self):
        await self.init()

        start_time = time.time()
        tasks = [self.sem_wrap(self.crawl_store, store) for store in self.branches]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                print(f"⚠️ Store task {i} failed with: {result}")

        elapsed = time.time() - start_time
        print(f"Total time: {elapsed:.2f} seconds")
        print("✅ Done.")

async def main():
    fetcher = WinMartFetcher(concurrency=2)
    await fetcher.init()
    await fetcher.run()

def run_sync():
    asyncio.run(main())

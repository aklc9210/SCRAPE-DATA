# fetch_product.py

import requests
import asyncio
import aiohttp  
from typing import List, Dict
from crawler.winmart.fetch_category import WinMartCategoryFetcher
from crawler.winmart.config import API_BASE_V3, HEADERS

class WinMartProductFetcher:
    def __init__(self):
        self.url = f"{API_BASE_V3}/item/category"
        self.headers = HEADERS
        self.categories = None
    
    async def init(self):
        self.categories = await WinMartCategoryFetcher().fetch_categories()

    async def fetch_products_by_store(self, store_id: str) -> List[Dict]:
        cats = self.categories
        allp = []
        for c in cats:
            # 1️⃣ In trước khi gọi
            print(f"▶ Fetching category {c['mapped_category']} (slug={c['slug']}) for store {store_id}")
            items = await self._by_cat(store_id, c)
            # 2️⃣ In sau khi có kết quả
            print(f"   • Retrieved {len(items)} items from category {c['mapped_category']}")
            allp += items
            await asyncio.sleep(0.1)
        return allp


    async def _by_cat(self, store_id: str, cat: Dict) -> List[Dict]:
        params = {
            "pageNumber": 1,
            "pageSize": 100,
            "slug": cat["slug"],
            "storeCode": store_id,
            "storeGroupCode": "1998"
        }

        async with aiohttp.ClientSession(headers=self.headers) as session:
            async with session.get(self.url, params=params, timeout=15) as resp:
                resp.raise_for_status()
                jd = await resp.json()  

        items = jd.get("data", {}).get("items", []) if isinstance(jd, dict) else jd

        out = []
        for it in items:
            norm = self._normalize(it, cat, store_id)
            if norm:
                out.append(norm)
        return out

    def _normalize(self, it: Dict, cat: Dict, store_id: str) -> Dict:
        product_id    = it.get("id", "")
        sku           = it.get("sku", "")
        if not product_id or not sku:
            return None

        name        = it.get("name", "").strip()
        price       = float(it.get("price", 0))
        sale_price  = float(it.get("salePrice", 0))
        current     = sale_price if sale_price > 0 else price

        return {
            "product_id": product_id,
            "sku": sku,
            "store_id": store_id,
            "name": name,
            "brand_name": it.get("brandName", "").strip(),
            "mapped_category": cat["mapped_category"],
            "price": current,
            "original_price": price,
            "sale_price": sale_price,
            "has_discount": sale_price > 0 < price,
            "uom": it.get("uom", ""),
            "quantity_per_unit": float(it.get("quantityPerUnit", 1)),
            "media_url": it.get("mediaUrl", ""),
            "promotion_text": it.get("promotionText", ""),
        }

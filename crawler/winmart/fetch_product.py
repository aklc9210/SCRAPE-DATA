import asyncio
import aiohttp
from typing import List, Dict
from crawler.winmart.config import API_BASE_V3, HEADERS

async def fetch_products_by_store(store_id: str, categories: List[Dict]) -> List[Dict]:
    """
    Fetch all products for a given store by iterating categories.
    """
    all_products = []
    for cat in categories:
        items = await fetch_products_by_category(store_id, cat)
        all_products.extend(items)
        await asyncio.sleep(0.1)
    return all_products

async def fetch_products_by_category(store_id: str, cat: Dict) -> List[Dict]:
    url = f"{API_BASE_V3}/item/category"
    params = {
        "pageNumber": 1,
        "pageSize": 100,
        "slug": cat["slug"],
        "storeCode": store_id,
        "storeGroupCode": "1998"
    }
    
    async with aiohttp.ClientSession(headers=HEADERS) as session:
        async with session.get(url, params=params, timeout=15) as resp:
            resp.raise_for_status()
            data = await resp.json()

    items = data.get("data", {}).get("items", []) if isinstance(data, dict) else data
    products = []
    for it in items:
        norm = normalize_product(it, cat, store_id)
        if norm:
            products.append(norm)
    return products

def normalize_product(it: Dict, cat: Dict, store_id: str) -> Dict:
    product_id = it.get("id", "")
    sku = it.get("sku", "")
    if not product_id or not sku:
        return None

    name = it.get("name", "").strip()
    price = float(it.get("price", 0))
    sale_price = float(it.get("salePrice", 0))
    current = sale_price if sale_price > 0 else price

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

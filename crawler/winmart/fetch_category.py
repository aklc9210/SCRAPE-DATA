import aiohttp
from typing import List, Dict
from crawler.winmart.config import API_BASE_V1, HEADERS
from crawler.process_data.process import CATEGORIES_MAPPING_WINMART

async def fetch_categories() -> List[Dict]:
    """
    Fetch WinMart categories and map them to English categories.
    """
    url = f"{API_BASE_V1}/category"
    async with aiohttp.ClientSession(headers=HEADERS) as session:
        async with session.get(url, timeout=15) as resp:
            resp.raise_for_status()
            data = await resp.json()

            if data.get("code") != "S200":
                raise Exception(f"API error: {data.get('message')}")
            return await _extract(data.get("data", []))

async def _extract(data: List[Dict]) -> List[Dict]:
    out = []
    for cat in data:
        parent = cat.get("parent", {})
        children = cat.get("lstChild", [])
        for node in [parent] + [c.get("parent", {}) for c in children]:
            name = node.get("name", "")
            slug = node.get("seoName", "")
            if slug and name in CATEGORIES_MAPPING_WINMART:
                out.append({
                    "name": name,
                    "code": node.get("code", ""),
                    "slug": slug,
                    "mapped_category": CATEGORIES_MAPPING_WINMART[name]
                })
    return out

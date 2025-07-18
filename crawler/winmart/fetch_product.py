import requests
import asyncio
import aiohttp
from typing import List, Dict, Optional
from crawler.winmart.fetch_category import WinMartCategoryFetcher

class WinMartProductFetcher:
    
    def __init__(self):
        self.session = aiohttp.ClientSession()
        self.api_base = "https://api-crownx.winmart.vn/it/api/web/v3"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'vi-VN,vi;q=0.9,en;q=0.8'
        }
        self.category_fetcher = WinMartCategoryFetcher()
    
    async def fetch_products_by_store(self, store_id: str, max_categories: int = None) -> List[Dict]:
        categories = self.category_fetcher.fetch_categories()
        if not categories:
            return []

        if max_categories:
            categories = categories[:max_categories]

        all_products = []

        for category in categories:
            products = await self._fetch_products_by_category(store_id, category)
            if products:
                all_products.extend(products)
            
            await asyncio.sleep(0.1)  

        return all_products

    
    async def _fetch_products_by_category(self, store_id: str, category: Dict) -> List[Dict]:
        url = f"{self.api_base}/item/category"
        
        params = {
            'pageNumber': 1,
            'pageSize': 100,  
            'slug': category['slug'],
            'storeCode': store_id,
            'storeGroupCode': '1998' 
        }
        
        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=15)
            
            if response.status_code != 200:
                raise Exception(f"API returned status {response.status_code}")
            
            data = response.json()
            return self._parse_products(data, category, store_id)
            
        except requests.RequestException as e:
            raise Exception(f"Request failed: {e}")
        except Exception as e:
            raise Exception(f"Parse failed: {e}")
    
    def _parse_products(self, data: Dict, category: Dict, store_id: str) -> List[Dict]:
        products = []
        
        # Extract items from the nested data structure
        items = []
        if isinstance(data, dict):
            nested_data = data.get('data', {})
            if isinstance(nested_data, dict):
                items = nested_data.get('items', [])
            else:
                items = data.get('items', data.get('products', []))
        elif isinstance(data, list):
            items = data
        
        for item in items:
            product = self._normalize_product_data(item, category, store_id)
            if product:
                products.append(product)
        
        return products
    
    def _normalize_product_data(self, item: Dict, category: Dict, store_id: str) -> Optional[Dict]:
        product_id = item.get('id', '')
        item_no = item.get('itemNo', '')
        name = item.get('name', '')
        description = item.get('description', '')
        
        if not product_id or not name:
            return None
        
        price = item.get('price', 0)
        sale_price = item.get('salePrice', 0)    
        current_price = sale_price if sale_price and sale_price > 0 else price
        brand_name = item.get('brandName', '')
        uom = item.get('uom', '')
        uom_name = item.get('uomName', '')
        quantity_per_unit = item.get('quantityPerUnit', 1.0)
        category_name = item.get('categoryName', '')
        sku = item.get('sku', '')
        media_url = item.get('mediaUrl', '')

        return {
            'product_id': product_id,
            'item_no': item_no,
            'store_id': store_id,
            'chain': 'winmart',
            'name': name.strip(),
            'description': description.strip(),
            'brand_name': brand_name.strip() if brand_name else '',
            'category_name': category_name or category['name'],
            'mapped_category': category.get('mapped_category', category['name']),
            'price': float(current_price) if current_price else 0.0,
            'original_price': float(price) if price else 0.0,
            'sale_price': float(sale_price) if sale_price else 0.0,
            'has_discount': bool(sale_price and sale_price > 0 and sale_price < price),
            'uom': uom,
            'uom_name': uom_name,
            'quantity_per_unit': float(quantity_per_unit) if quantity_per_unit else 1.0,
            'sku': sku,
            'media_url': media_url
        }

async def fetch_products_for_store(store_id: str) -> List[Dict]:
    fetcher = WinMartProductFetcher()
    return await fetcher.fetch_products_by_store(store_id)


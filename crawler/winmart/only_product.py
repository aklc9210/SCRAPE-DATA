import asyncio
from datetime import datetime
from pathlib import Path
from crawler.winmart.fetch_product import WinMartProductFetcher
from crawler.winmart.process_data import DataProcessor
from db import MongoDB
from pymongo import UpdateOne
from tqdm.asyncio import tqdm  
from tqdm import tqdm as sync_tqdm 

class ProductOnlyFetcher:
    
    def __init__(self, batch_size: int = 50):
        self.product_fetcher = WinMartProductFetcher()
        self.processor = DataProcessor()
        self.db = MongoDB.get_db()
        self.batch_size = batch_size
    
    async def fetch_products_only(self, store_ids: list = None):
        if not store_ids:
            from crawler.winmart.fetch_branches import get_active_stores
            stores = get_active_stores()
            store_ids = [store['store_id'] for store in stores[65:80]]
            # ['4091', '6001', '6705', '4405']
            print(store_ids)
        print(f"🚀 Starting product fetch for {len(store_ids)} stores (batch size: {self.batch_size})")
        
        for store_id in tqdm(store_ids, desc="Processing stores", unit="store"):
            try:
                print(f"\n📦 Fetching products from store: {store_id}")
                raw_products = await self.product_fetcher.fetch_products_by_store(store_id)
                print(f"Fetched {len(raw_products)} products from store {store_id}")
                
                await self._process_and_save_in_batches(raw_products, store_id)
                        
            except Exception as e:
                print(f"Error processing store {store_id}: {e}")
                continue
    
    async def _process_and_save_in_batches(self, raw_products: list, store_id: str):
        """Process and save products in batches to avoid data loss"""
        total_processed = 0
        total_saved = 0
        
        # Process products in batches
        for i in range(0, len(raw_products), self.batch_size):
            batch = raw_products[i:i + self.batch_size]
            batch_num = (i // self.batch_size) + 1
            total_batches = (len(raw_products) + self.batch_size - 1) // self.batch_size
            
            # print(f"🔄 Processing batch {batch_num}/{total_batches} ({len(batch)} products)")
            
            try:
                # Process current batch
                processed_products = []
                for raw_product in sync_tqdm(batch, 
                                            desc=f"Processing batch {batch_num}/{total_batches}", 
                                            unit="product", 
                                            leave=False):
                    processed = self._process_winmart_product(raw_product, store_id)
                    if processed:
                        processed_products.append(processed)
                        total_processed += 1
                
                if processed_products:
                    saved_count = await self._save_products_to_db(processed_products, store_id, batch_num)
                    total_saved += saved_count
                    # print(f"Batch {batch_num}: Processed {len(processed_products)}, Saved {saved_count} products")
                else:
                    print(f"Batch {batch_num}: No valid products to save")
                
                await asyncio.sleep(0.5)
                
            except Exception as e:
                print(f"Error processing batch {batch_num} for store {store_id}: {e}")
                continue
        
        print(f"Store {store_id} complete: {total_processed} processed, {total_saved} saved to database")
    
    def _map_category(self, original_category: str) -> str:
        """Map WinMart category to standard category using exact mapping"""
        # WinMart category mapping to standardized names
        category_mapping = {
            # Milk & Dairy products
            "Sữa các loại": "Milk",
            "Sữa Tươi": "Milk",
            "Sữa Hạt - Sữa Đậu": "Milk",
            "Sữa Bột": "Milk",
            "Bơ Sữa - Phô Mai": "Milk",
            "Sữa đặc": "Milk",
            "Sữa Chua - Váng Sữa": "Yogurt",
            "Sữa Bột - Sữa Dinh Dưỡng": "Milk",
            
            # Vegetables & Fruits
            "Rau - Củ - Trái Cây": "Vegetables",
            "Rau Lá": "Vegetables",
            "Củ, Quả": "Vegetables",
            "Trái cây tươi": "Fresh Fruits",
            
            # Meat, Seafood, Eggs
            "Thịt - Hải Sản Tươi": "Fresh Meat",
            "Thịt": "Fresh Meat",
            "Hải Sản": "Seafood & Fish Balls",
            "Trứng - Đậu Hũ": "Fresh Meat",
            "Trứng": "Fresh Meat",
            "Đậu hũ": "Instant Foods",
            "Thịt Đông Lạnh": "Instant Foods",
            "Hải Sản Đông Lạnh": "Instant Foods",
            
            # Bakery & Confectionery
            "Bánh Kẹo": "Cakes",
            "Bánh Xốp - Bánh Quy": "Cakes",
            "Kẹo - Chocolate": "Candies",
            "Bánh Snack": "Snacks",
            "Hạt - Trái Cây Sấy Khô": "Dried Fruits",
            
            # Beverages
            "Đồ uống có cồn": "Alcoholic Beverages",
            "Bia": "Alcoholic Beverages",
            "Đồ Uống - Giải Khát": "Beverages",
            "Cà Phê": "Beverages",
            "Nước Suối": "Beverages",
            "Nước Ngọt": "Beverages",
            "Trà - Các Loại Khác": "Beverages",
            
            # Instant Foods
            "Mì - Thực Phẩm Ăn Liền": "Instant Foods",
            "Mì": "Instant Foods",
            "Miến - Hủ Tíu - Bánh Canh": "Instant Foods",
            "Cháo": "Instant Foods",
            "Phở - Bún": "Instant Foods",
            
            # Dry Foods & Grains
            "Thực Phẩm Khô": "Grains & Staples",
            "Gạo - Nông Sản Khô": "Grains & Staples",
            "Ngũ Cốc - Yến Mạch": "Cereals & Grains",
            "Thực Phẩm Đóng Hộp": "Instant Foods",
            "Rong Biển - Tảo Biển": "Snacks",
            "Bột Các Loại": "Grains & Staples",
            "Thực Phẩm Chay": "Instant Foods",
            
            # Processed Foods
            "Thực Phẩm Chế Biến": "Instant Foods",
            "Bánh mì": "Instant Foods",
            "Xúc xích - Thịt Nguội": "Cold Cuts: Sausages & Ham",
            "Bánh bao": "Instant Foods",
            "Kim chi": "Instant Foods",
            "Thực Phẩm Chế Biến Khác": "Instant Foods",
            
            # Seasonings
            "Gia vị": "Seasonings",
            "Dầu Ăn": "Seasonings",
            "Nước Mắm - Nước Chấm": "Seasonings",
            "Đường": "Seasonings",
            "Nước Tương": "Seasonings",
            "Hạt Nêm": "Seasonings",
            "Tương Các Loại": "Seasonings",
            "Gia Vị Khác": "Seasonings",
            
            # Frozen Foods
            "Thực Phẩm Đông Lạnh": "Instant Foods",
            "Chả Giò": "Instant Foods",
            "Cá - Bò Viên": "Instant Foods",
            "Thực Phẩm Đông Lạnh Khác": "Instant Foods"
        }
        
        if not original_category:
            return None 
        
        mapped = category_mapping.get(original_category)
        
        return mapped
    
    async def _save_products_to_db(self, products: list, store_id: str = None, batch_num: int = None) -> int:
        """Save products to database grouped by category like BHX"""
        # Group by mapped category
        category_groups = {}
        for product in products:
            category = product.get("category", "General")
            coll_name = category.replace(" ", "_").lower()
            if coll_name not in category_groups:
                category_groups[coll_name] = []
            category_groups[coll_name].append(product)
        
        total_saved = 0
        
        # Bulk insert by category
        for coll_name, products_list in category_groups.items():
            collection = self.db[coll_name]
            operations = []
            
            for product in products_list:
                operations.append(
                    UpdateOne(
                        {"sku": product["sku"], "store_id": product["store_id"]},
                        {"$set": product},
                        upsert=True
                    )
                )
            
            if operations:
                try:
                    result = collection.bulk_write(operations, ordered=False)
                    saved_count = result.upserted_count + result.modified_count
                    total_saved += saved_count
                    
                    batch_info = f" (Batch {batch_num})" if batch_num else ""
                    store_info = f" Store {store_id}" if store_id else ""
                    # print(f"{store_info}{batch_info}: {saved_count} products → {coll_name}")
                    
                except Exception as e:
                    print(f"Error saving to {coll_name}: {e}")
                    continue
        
        return total_saved
    
    def _process_winmart_product(self, product: dict, store_id: str) -> dict:
        try:
            english_name = self.processor.translate_vi2en(product.get("name", ""))
            if not english_name:
                return None
            
            # Generate search tokens
            token_ngrams = self.processor.generate_token_ngrams(english_name, 2)
            
            price = product.get('price', 0)
            original_price = product.get('original_price', price)
            sale_price = product.get('sale_price', price)
            
            has_discount = sale_price < original_price if original_price > 0 else False
            discount_percent = ((original_price - sale_price) / original_price * 100) if original_price > 0 else 0
            
            unit = product.get('uom', 'piece')
            net_value = product.get('quantity_per_unit', 1)
            normalized_net_value, normalized_unit = self.processor.normalize_net_value(
                unit, net_value, product.get('name', '')
            )
            
            original_category = product.get("category_name", "")
            mapped_category = self._map_category(original_category)
            
            if not mapped_category:
                return None
            
            return {
                "sku": product.get("product_id", "") or product.get("sku", ""),
                "name": product.get("name", ""),
                "name_en": english_name,
                "unit": normalized_unit.lower(),
                "netUnitValue": normalized_net_value,
                "token_ngrams": token_ngrams,
                "category": mapped_category,
                "store_id": store_id,
                "url": product.get("media_url", ""),
                "image": product.get("image", "") or product.get("media_url", ""),
                "promotion": product.get("promotion_text", ""),
                "price": float(sale_price) if sale_price else 0.0,
                "sysPrice": float(original_price) if original_price else 0.0,
                "discountPercent": round(discount_percent, 2),
                "date_begin": datetime.now().strftime("%Y-%m-%d"),
                "date_end": datetime.now().strftime("%Y-%m-%d"),
                "crawled_at": datetime.utcnow().isoformat(),
                
                "chain": "WM",
                "brand_name": product.get("brand_name", ""),
                "item_no": product.get("item_no", ""),
                "description": product.get("description", ""),
                "original_category": original_category,
                "has_discount": has_discount
            }
            
        except Exception as e:
            if "translation" in str(e).lower() and hash(product.get('name', '')) % 20 == 0:
                print(f" Error processing product {product.get('name', 'unknown')}: {e}")
            return None

async def main():
    fetcher = ProductOnlyFetcher(batch_size=50)     
    await fetcher.fetch_products_only()
    
if __name__ == "__main__":
    asyncio.run(main())
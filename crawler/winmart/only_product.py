import asyncio
import csv
from datetime import datetime
from pathlib import Path
from fetch_product import WinMartProductFetcher
from process_data import DataProcessor

class ProductOnlyFetcher:
    """Fetch products with full data processing and save to CSV"""
    
    def __init__(self, output_dir: str = "output"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.product_fetcher = WinMartProductFetcher()
        self.processor = DataProcessor()
    
    async def fetch_products_only(self, store_ids: list = None) -> str:
        """Fetch and process products for specified stores"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        products_file = self.output_dir / f"winmart_products_processed_{timestamp}.csv"
        
        # Default store IDs if not provided
        if not store_ids:
            from fetch_branches import get_active_stores
            stores = get_active_stores()
            store_ids = [store['store_id'] for store in stores]
        
        # Extended fieldnames with processed data
        fieldnames = [
            'product_id', 'item_no', 'store_id', 'chain', 'name', 'name_en', 
            'normalized_name', 'description', 'brand_name', 'category_name', 
            'mapped_category', 'price', 'original_price', 'sale_price', 
            'has_discount', 'discount_percent', 'uom', 'uom_name', 
            'quantity_per_unit', 'net_unit_value', 'sku', 'media_url',
            'promotion', 'token_ngrams', 'url', 'date_begin', 'date_end'
        ]
        
        product_count = 0
        
        with open(products_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for store_id in store_ids:
                try:
                    # Fetch raw products
                    raw_products = await self.product_fetcher.fetch_products_by_store(store_id)
                    
                    for raw_product in raw_products:
                        # Process each product
                        processed = self._process_winmart_product(raw_product, store_id)
                        
                        if processed:
                            row = {field: processed.get(field, '') for field in fieldnames}
                            
                            # Handle special data types
                            if isinstance(row['has_discount'], bool):
                                row['has_discount'] = str(row['has_discount']).lower()
                            
                            # Convert token_ngrams list to string
                            if isinstance(row['token_ngrams'], list):
                                row['token_ngrams'] = ','.join(row['token_ngrams'])
                            
                            writer.writerow(row)
                            product_count += 1
                        
                except Exception as e:
                    print(f"Error processing store {store_id}: {e}")
                    continue
        
        return str(products_file)
    
    def _process_winmart_product(self, product: dict, store_id: str) -> dict:
        """Process WinMart product with full data transformations"""
        try:
            # Translate name to English
            english_name = self.processor.translate_vi2en(product.get("name", ""))
            if not english_name:
                return None
            
            # Generate search tokens
            token_ngrams = self.processor.generate_token_ngrams(english_name, 2)
            
            # Extract price info (adapted for WinMart structure)
            price = product.get('price', 0)
            original_price = product.get('original_price', price)
            sale_price = product.get('sale_price', price)
            
            # Calculate discount
            has_discount = sale_price < original_price if original_price > 0 else False
            discount_percent = ((original_price - sale_price) / original_price * 100) if original_price > 0 else 0
            
            # Process unit and net value
            unit = product.get('uom', 'piece')
            net_value = product.get('quantity_per_unit', 1)
            normalized_net_value, normalized_unit = self.processor.normalize_net_value(
                unit, net_value, product.get('name', '')
            )
            
            # Build processed product
            processed = {
                "product_id": product.get("product_id", ""),
                "item_no": product.get("item_no", ""),
                "store_id": store_id,
                "chain": "winmart",
                "name": product.get("name", ""),
                "name_en": english_name,
                "normalized_name": self.processor.normalize_name(english_name),
                "description": product.get("description", ""),
                "brand_name": product.get("brand_name", ""),
                "category_name": product.get("category_name", ""),
                "mapped_category": product.get("mapped_category", ""),
                "price": float(price) if price else 0.0,
                "original_price": float(original_price) if original_price else 0.0,
                "sale_price": float(sale_price) if sale_price else 0.0,
                "has_discount": has_discount,
                "discount_percent": round(discount_percent, 2),
                "uom": unit,
                "uom_name": product.get("uom_name", ""),
                "quantity_per_unit": float(net_value) if net_value else 1.0,
                "net_unit_value": normalized_net_value,
                "sku": product.get("sku", ""),
                "media_url": product.get("media_url", ""),
                "promotion": "",  # WinMart doesn't have promotion text in current structure
                "token_ngrams": token_ngrams,
                "url": "",  # WinMart doesn't have product URLs in current structure
                "date_begin": "",
                "date_end": "",
            }
            
            return processed
            
        except Exception as e:
            print(f"Error processing product {product.get('name', 'unknown')}: {e}")
            return None
    
    async def fetch_and_process_specific_stores(self, store_ids: list, max_categories: int = None) -> str:
        """Fetch products with category limit for testing"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        products_file = self.output_dir / f"winmart_products_test_{timestamp}.csv"
        
        fieldnames = [
            'product_id', 'item_no', 'store_id', 'chain', 'name', 'name_en', 
            'normalized_name', 'description', 'brand_name', 'category_name', 
            'mapped_category', 'price', 'original_price', 'sale_price', 
            'has_discount', 'discount_percent', 'uom', 'uom_name', 
            'quantity_per_unit', 'net_unit_value', 'sku', 'media_url',
            'promotion', 'token_ngrams', 'url', 'date_begin', 'date_end'
        ]
        
        product_count = 0
        
        with open(products_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for store_id in store_ids:
                try:
                    # Fetch with category limit
                    raw_products = await self.product_fetcher.fetch_products_by_store(
                        store_id, max_categories
                    )
                    
                    for raw_product in raw_products:
                        processed = self._process_winmart_product(raw_product, store_id)
                        
                        if processed:
                            row = {field: processed.get(field, '') for field in fieldnames}
                            
                            if isinstance(row['has_discount'], bool):
                                row['has_discount'] = str(row['has_discount']).lower()
                            
                            if isinstance(row['token_ngrams'], list):
                                row['token_ngrams'] = ','.join(row['token_ngrams'])
                            
                            writer.writerow(row)
                            product_count += 1
                        
                except Exception as e:
                    print(f"Error processing store {store_id}: {e}")
                    continue
        
        print(f"Processed {product_count} products and saved to {products_file}")
        return str(products_file)

async def main():
    """Main function"""
    fetcher = ProductOnlyFetcher()
    
    print("Choose option:")
    print("1. Test with specific stores (limited categories)")
    print("2. Full collection for all active stores")
    
    choice = input("Enter choice (1-2): ").strip()
    
    if choice == "1":
        # Test mode
        test_stores = ["1683"]
        products_file = await fetcher.fetch_and_process_specific_stores(
            test_stores, max_categories=3
        )
    else:
        # All active stores
        products_file = await fetcher.fetch_products_only()
    
    print(f"Products saved to: {products_file}")

if __name__ == "__main__":
    asyncio.run(main())
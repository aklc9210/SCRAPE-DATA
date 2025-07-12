import re
import unicodedata
from typing import List, Dict, Tuple, Set
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
import torch
from deep_translator import GoogleTranslator
import time
import random

class DataProcessor:
    """Process and normalize product data with duplicate checking"""
    
    def __init__(self, db=None):
        # Initialize translation model
        torch.set_num_threads(15)
        self.tokenizer_vi2en = AutoTokenizer.from_pretrained(
            "vinai/vinai-translate-vi2en-v2",
            use_fast=False,
            src_lang="vi_VN",     
            tgt_lang="en_XX",
            legacy=False  # Use new tokenizer behavior
        )
        self.model_vi2en = AutoModelForSeq2SeqLM.from_pretrained("vinai/vinai-translate-vi2en-v2")
        self.db = db
    
    def translate_vi2en(self, vi_text: str) -> str:
        """Translate Vietnamese text to English"""
        if not vi_text or not vi_text.strip():
            return ""
        
        try:
            inputs = self.tokenizer_vi2en(vi_text, return_tensors="pt")
            decoder_start_token_id = self.tokenizer_vi2en.lang_code_to_id["en_XX"]
            outputs = self.model_vi2en.generate(
                **inputs,
                decoder_start_token_id=decoder_start_token_id,
                num_beams=5,
                early_stopping=True
            )
            return self.tokenizer_vi2en.decode(outputs[0], skip_special_tokens=True)
        except Exception:
            return ""

    # def __init__(self):
    #     # Initialize Google Translator
    #     self.translator = GoogleTranslator(source='vi', target='en')
        
    # def translate_vi2en(self, vi_text: str) -> str:
    #     """Translate Vietnamese text to English using Google Translate"""
    #     if not vi_text or not vi_text.strip():
    #         return ""
        
    #     try:
    #         # Add small delay to avoid rate limiting
    #         # time.sleep(random.uniform(0.1, 0.3))
            
    #         # Translate text
    #         result = self.translator.translate(vi_text)
    #         return result if result else ""
            
    #     except Exception as e:
    #         print(f"Translation error: {e}")
    #         return ""
    
    def get_existing_product_ids(self, category_title: str, store_id: str) -> Set[str]:
        """Get set of existing product IDs for a category and store"""
        if not self.db:
            return set()
            
        try:
            coll_name = category_title.replace(" ", "_").lower()
            collection = self.db[coll_name]
            
            # Get all existing product_ids for this store
            existing_docs = collection.find(
                {"store_id": store_id}, 
                {"product_id": 1, "_id": 0}
            )
            
            existing_ids = {doc["product_id"] for doc in existing_docs if doc.get("product_id")}
            print(f"Found {len(existing_ids)} existing products in {coll_name} for store {store_id}")
            return existing_ids
            
        except Exception as e:
            print(f"Error getting existing product IDs: {e}")
            return set()
    
    def get_existing_skus(self, category_title: str, store_id: str) -> Set[str]:
        """Get set of existing SKUs for a category and store (alternative to product_id)"""
        if not self.db:
            return set()
            
        try:
            coll_name = category_title.replace(" ", "_").lower()
            collection = self.db[coll_name]
            
            # Get all existing SKUs for this store
            existing_docs = collection.find(
                {"store_id": store_id}, 
                {"sku": 1, "_id": 0}
            )
            
            existing_skus = {doc["sku"] for doc in existing_docs if doc.get("sku")}
            print(f"Found {len(existing_skus)} existing SKUs in {coll_name} for store {store_id}")
            return existing_skus
            
        except Exception as e:
            print(f"Error getting existing SKUs: {e}")
            return set()
    
    def filter_new_products(self, products: List[dict], category_title: str, store_id: str, 
                           use_sku: bool = False) -> List[dict]:
        """Filter out products that already exist in database"""
        if not self.db or not products:
            return products
        
        if use_sku:
            existing_identifiers = self.get_existing_skus(category_title, store_id)
            id_field = "sku"
        else:
            existing_identifiers = self.get_existing_product_ids(category_title, store_id)
            id_field = "id"
        
        # Filter products
        new_products = []
        for product in products:
            product_identifier = product.get(id_field, "")
            if product_identifier and product_identifier not in existing_identifiers:
                new_products.append(product)
        
        print(f"Filtered {len(products)} products -> {len(new_products)} new products to process")
        return new_products
    
    def should_skip_product(self, product: dict, category_title: str, store_id: str, 
                           use_sku: bool = False) -> bool:
        """Check if a single product should be skipped (already exists)"""
        if not self.db:
            return False
            
        try:
            coll_name = category_title.replace(" ", "_").lower()
            collection = self.db[coll_name]
            
            if use_sku:
                query = {"store_id": store_id, "sku": product.get("sku", "")}
            else:
                query = {"store_id": store_id, "product_id": product.get("id", "")}
            
            existing = collection.find_one(query, {"_id": 1})
            return existing is not None
            
        except Exception as e:
            print(f"Error checking product existence: {e}")
            return False
    
    def extract_net_value_and_unit_from_name(self, name: str, fallback_unit: str) -> Tuple[float, str]:
        """Extract numeric value and unit from product name"""
        tmp_name = name.lower()
        matches = re.findall(r"(\d+(?:\.\d+)?)\s*(g|ml|lít|kg|gói|l)\b", tmp_name)
        if matches:
            value, unit = matches[-1]  # use the LAST match
            return float(value), unit
        return 1.0, fallback_unit
    
    def normalize_net_value(self, unit: str, net_value: float, name: str) -> Tuple[float, str]:
        """Normalize net value and unit based on product name"""
        unit = unit.lower()
        name_lower = name.lower()

        # Convert kg to g, lít to ml
        if unit == "kg":
            return float(net_value) * 1000, "g"
        elif unit == "lít":
            return float(net_value) * 1000, "ml"
        
        # Check for kg in name if unit is not kg/g/ml/lít
        if unit not in ["kg", "g", "ml", "lít"]:
            match_kg = re.search(r"(\d+(?:\.\d+)?)\s*kg", name_lower)
            if match_kg:
                value = float(match_kg.group(1))
                return value * 1000, unit
        
        # Special case: túi 1kg
        if unit == "túi 1kg":
            return float(net_value) * 1000, "túi"
        
        # túi with fruits - assume 0.7kg
        if unit == "túi" and "trái" in name_lower:
            return 0.7 * 1000, unit

        # hộp or vỉ with eggs count
        if unit in ["hộp", "vỉ"] and "quả" in name_lower:
            matches = re.findall(rf"{unit}\s*(\d+)", name_lower)
            if matches:
                return sum(map(int, matches)), unit

        # thùng/lốc X items of Y ml/g each
        match_pack = re.search(r"(thùng|lốc)\s*(\d+).*?(\d+(?:\.\d+)?)\s*(g|ml)", name_lower)
        if match_pack:
            count = int(match_pack.group(2))
            per_item = float(match_pack.group(3))
            return count * per_item, unit

        # Fallback: extract from name
        extracted_value, _ = self.extract_net_value_and_unit_from_name(name, unit)
        if extracted_value > 0:
            return extracted_value, unit

        return float(net_value) if net_value != 0 else 1.0, unit
    
    def extract_best_price(self, product: dict) -> dict:
        """Extract best price information from product data"""
        base_price_info = product.get("productPrices", [])
        campaign_info = product.get("lstCampaingInfo", [])
        name = product.get("name", "")
        original_unit = product.get("unit", "").lower()
        
        def build_result(info: dict, unit: str, net_value: float):
            return {
                "name": name,
                "unit": unit, 
                "netUnitValue": net_value,
                "price": info.get("price"),
                "sysPrice": info.get("sysPrice"),
                "discountPercent": info.get("discountPercent"),
                "date_begin": info.get("startTime") or info.get("poDate"),
                "date_end": info.get("dueTime") or info.get("poDate"),
            }
        
        # Priority 1: Campaign price
        if campaign_info:
            campaign = campaign_info[0]
            campaign_price = campaign.get("productPrice", {})
            net_value = campaign_price.get("netUnitValue", 0)
            net_value, converted_unit = self.normalize_net_value(original_unit, net_value, name)
            return build_result(campaign_price, converted_unit, net_value)

        # Priority 2: Base price
        if base_price_info:
            price_info = base_price_info[0]
            net_value = price_info.get("netUnitValue", 0)
            net_value, converted_unit = self.normalize_net_value(original_unit, net_value, name)
            return build_result(price_info, converted_unit, net_value)

        # Fallback: no price info
        return {
            "name": name,
            "unit": original_unit,
            "netUnitValue": 1.0,
            "price": None,
            "sysPrice": None,
            "discountPercent": None,
            "date_begin": None,
            "date_end": None,
        }
    
    def tokenize_by_whitespace(self, text: str) -> List[str]:
        """Tokenize text by whitespace, filter tokens >= 2 chars"""
        if text is None:
            return []
        return [token for token in text.lower().split() if len(token) >= 2]

    def generate_ngrams(self, token: str, n: int) -> List[str]:
        """Generate n-grams from a token"""
        if token is None or len(token) < n:
            return []
        return [token[i:i+n] for i in range(len(token) - n + 1)]

    def generate_token_ngrams(self, text: str, n: int) -> List[str]:
        """Generate n-grams from all tokens in text"""
        tokens = self.tokenize_by_whitespace(text)
        ngrams = []
        for token in tokens:
            ngrams.extend(self.generate_ngrams(token, n))
        return ngrams
    
    def normalize_name(self, name: str) -> str:
        """Convert Vietnamese to ASCII + lowercase, remove punctuation"""
        if not name:
            return ""
        
        nfkd = unicodedata.normalize("NFKD", name)
        ascii_str = "".join([c for c in nfkd if not unicodedata.combining(c)])
        return re.sub(r"[^\w\s-]", "", ascii_str).lower().strip()
    
    def process_product(self, product: dict, category_title: str, store_id: str, 
                       skip_existing: bool = True, use_sku: bool = False) -> dict:
        """Process a single product with all transformations"""
        
        # Check if product already exists and should be skipped
        if skip_existing and self.should_skip_product(product, category_title, store_id, use_sku):
            print(f"Skipping existing product: {product.get('name', 'Unknown')}")
            return None
        
        # Translate name
        english_name = self.translate_vi2en(product.get("name", ""))
        if not english_name:
            return None
        
        # Extract price info
        price_info = self.extract_best_price(product)
        
        # Generate search tokens
        token_ngrams = self.generate_token_ngrams(english_name, 2)
        
        # Build processed product
        processed = {
            "product_id": product.get("id", ""),
            "store_id": store_id,
            "chain": "winmart",  # or extract from context
            "name": product.get("name", ""),
            "name_en": english_name,
            "normalized_name": self.normalize_name(english_name),
            "description": product.get("description", ""),
            "category_name": category_title,
            "unit": price_info["unit"].lower(),
            "net_unit_value": price_info["netUnitValue"],
            "price": price_info["price"],
            "original_price": price_info["sysPrice"],
            "sale_price": price_info["price"],
            "has_discount": bool(price_info.get("discountPercent", 0) > 0),
            "discount_percent": price_info.get("discountPercent", 0),
            "date_begin": price_info["date_begin"],
            "date_end": price_info["date_end"],
            "sku": product.get("sku", ""),
            "url": product.get("avatar", ""),
            "token_ngrams": token_ngrams,
        }
        
        return processed
    
    def process_products_batch(self, products: List[dict], category_title: str, store_id: str,
                              skip_existing: bool = True, use_sku: bool = False) -> List[dict]:
        """Process multiple products at once with duplicate checking"""
        
        # Filter out existing products first for better performance
        if skip_existing:
            products = self.filter_new_products(products, category_title, store_id, use_sku)
            if not products:
                print("No new products to process after filtering")
                return []
        
        processed_products = []
        
        for product in products:
            try:
                # Since we already filtered, we can skip the individual check
                processed = self.process_product(
                    product, category_title, store_id, 
                    skip_existing=False, use_sku=use_sku
                )
                if processed:
                    processed_products.append(processed)
            except Exception as e:
                print(f"Error processing product {product.get('name', 'Unknown')}: {e}")
                continue
        
        print(f"Successfully processed {len(processed_products)} products")
        return processed_products
    
    def get_collection_stats(self, category_title: str) -> Dict[str, int]:
        """Get statistics for a category collection"""
        if not self.db:
            return {}
            
        try:
            coll_name = category_title.replace(" ", "_").lower()
            collection = self.db[coll_name]
            
            total_count = collection.count_documents({})
            store_counts = {}
            
            # Get count by store
            pipeline = [
                {"$group": {"_id": "$store_id", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
            
            for doc in collection.aggregate(pipeline):
                store_counts[doc["_id"]] = doc["count"]
            
            return {
                "total_products": total_count,
                "stores": store_counts
            }
            
        except Exception as e:
            print(f"Error getting collection stats: {e}")
            return {}

# Convenience functions
def create_processor(db=None) -> DataProcessor:
    """Create a data processor instance"""
    return DataProcessor(db=db)

def process_single_product(product: dict, category: str, store_id: str, 
                          db=None, skip_existing: bool = True) -> dict:
    """Process a single product (convenience function)"""
    processor = DataProcessor(db=db)
    return processor.process_product(product, category, store_id, skip_existing)

def process_products_batch(products: List[dict], category: str, store_id: str, 
                          db=None, skip_existing: bool = True) -> List[dict]:
    """Process multiple products (convenience function)"""
    processor = DataProcessor(db=db)
    return processor.process_products_batch(products, category, store_id, skip_existing)

def translate_text(text: str) -> str:
    """Translate Vietnamese text to English (convenience function)"""
    processor = DataProcessor()
    return processor.translate_vi2en(text)

if __name__ == "__main__":
    # Test the processor
    processor = DataProcessor()

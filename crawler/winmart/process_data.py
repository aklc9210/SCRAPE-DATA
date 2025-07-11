import re
import unicodedata
from typing import List, Dict, Tuple
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
import torch

class DataProcessor:
    """Process and normalize product data"""
    
    def __init__(self):
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
    
    def process_product(self, product: dict, category_title: str, store_id: str) -> dict:
        """Process a single product with all transformations"""
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
    
    def process_products_batch(self, products: List[dict], category_title: str, store_id: str) -> List[dict]:
        """Process multiple products at once"""
        processed_products = []
        
        for product in products:
            try:
                processed = self.process_product(product, category_title, store_id)
                if processed:
                    processed_products.append(processed)
            except Exception:
                continue
        
        return processed_products

# Convenience functions
def create_processor() -> DataProcessor:
    """Create a data processor instance"""
    return DataProcessor()

def process_single_product(product: dict, category: str, store_id: str) -> dict:
    """Process a single product (convenience function)"""
    processor = create_processor()
    return processor.process_product(product, category, store_id)

def translate_text(text: str) -> str:
    """Translate Vietnamese text to English (convenience function)"""
    processor = create_processor()
    return processor.translate_vi2en(text)

if __name__ == "__main__":
    # Test the processor
    processor = DataProcessor()
    
    # Test translation
    test_text = "Bánh mì sandwich thịt nguội"
    english = processor.translate_vi2en(test_text)
    print(f"Vietnamese: {test_text}")
    print(f"English: {english}")
    
    # Test product processing
    sample_product = {
        "id": "12345",
        "name": "Sữa tươi TH True Milk 1 lít",
        "unit": "hộp",
        "productPrices": [{
            "price": 25000,
            "sysPrice": 27000,
            "netUnitValue": 1000,
            "discountPercent": 7.4
        }]
    }
    
    processed = processor.process_product(sample_product, "Milk", "1683")
    print(f"\nProcessed product:")
    for key, value in processed.items():
        print(f"  {key}: {value}")
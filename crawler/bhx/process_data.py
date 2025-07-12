import re
from typing import List, Dict
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
from datetime import datetime
from pymongo import UpdateOne

# ===== TRANSLATION SETUP =====
tokenizer_vi2en = AutoTokenizer.from_pretrained(
    "vinai/vinai-translate-vi2en-v2",
    use_fast=False,
    src_lang="vi_VN",
    tgt_lang="en_XX"
)
model_vi2en = AutoModelForSeq2SeqLM.from_pretrained("vinai/vinai-translate-vi2en-v2")

# ===== CATEGORIES MAPPING =====
VALID_TITLES = {
    # Thịt, cá, trứng
    "Thịt heo", "Thịt bò", "Thịt gà, vịt, chim", "Thịt sơ chế", "Trứng gà, vịt, cút",
    "Cá, hải sản, khô", "Cá hộp", "Lạp xưởng", "Xúc xích", "Heo, bò, pate hộp",
    "Chả giò, chả ram", "Chả lụa, thịt nguội", "Xúc xích, lạp xưởng tươi",
    "Cá viên, bò viên", "Thịt, cá đông lạnh",

    # Rau, củ, quả, nấm
    "Trái cây", "Rau lá", "Củ, quả", "Nấm các loại", "Rau, củ làm sẵn",
    "Rau củ đông lạnh",

    # Đồ ăn chay
    "Đồ chay ăn liền", "Đậu hũ, đồ chay khác", "Đậu hũ, tàu hũ",

    # Ngũ cốc, tinh bột
    "Ngũ cốc", "Ngũ cốc, yến mạch", "Gạo các loại", "Bột các loại",
    "Đậu, nấm, đồ khô",

    # Mì, bún, phở, cháo
    "Mì ăn liền", "Phở, bún ăn liền", "Hủ tiếu, miến", "Miến, hủ tiếu, phở khô",
    "Mì Ý, mì trứng", "Cháo gói, cháo tươi", "Bún các loại", "Nui các loại",
    "Bánh tráng các loại", "Bánh phồng, bánh đa", "Bánh gạo Hàn Quốc",

    # Gia vị, phụ gia, dầu
    "Nước mắm", "Nước tương", "Tương, chao các loại", "Tương ớt - đen, mayonnaise",
    "Dầu ăn", "Dầu hào, giấm, bơ", "Gia vị nêm sẵn", "Muối",
    "Hạt nêm, bột ngọt, bột canh", "Tiêu, sa tế, ớt bột", "Bột nghệ, tỏi, hồi, quế,...",
    "Nước chấm, mắm", "Mật ong, bột nghệ",

    # Sữa & các sản phẩm từ sữa
    "Sữa tươi", "Sữa đặc", "Sữa pha sẵn", "Sữa hạt, sữa đậu", "Sữa ca cao, lúa mạch",
    "Sữa trái cây, trà sữa", "Sữa chua ăn", "Sữa chua uống liền", "Bơ sữa, phô mai",

    # Đồ uống
    "Bia, nước có cồn", "Rượu", "Nước trà", "Nước ngọt", "Nước ép trái cây",
    "Nước yến", "Nước tăng lực, bù khoáng", "Nước suối", "Cà phê hoà tan",
    "Cà phê pha phin", "Cà phê lon", "Trà khô, túi lọc",

    # Bánh kẹo, snack
    "Bánh tươi, Sandwich", "Bánh bông lan", "Bánh quy", "Bánh snack, rong biển",
    "Bánh Chocopie", "Bánh gạo", "Bánh quế", "Bánh que", "Bánh xốp",
    "Kẹo cứng", "Kẹo dẻo, kẹo marshmallow", "Kẹo singum", "Socola",
    "Trái cây sấy", "Hạt khô", "Rong biển các loại", "Rau câu, thạch dừa",
    "Mứt trái cây", "Cơm cháy, bánh tráng",

    # Món ăn chế biến sẵn, đông lạnh
    "Làm sẵn, ăn liền", "Sơ chế, tẩm ướp", "Nước lẩu, viên thả lẩu",
    "Kim chi, đồ chua", "Mandu, há cảo, sủi cảo", "Bánh bao, bánh mì, pizza",
    "Kem cây, kem hộp", "Bánh flan, thạch, chè", "Trái cây hộp, siro",

    "Cá mắm, dưa mắm", "Đường", "Nước cốt dừa lon", "Sữa chua uống", "Khô chế biến sẵn"
}

CATEGORIES_MAPPING = {
    # Thịt, cá, trứng
    "Thịt heo": "Fresh Meat",
    "Thịt bò": "Fresh Meat", 
    "Thịt gà, vịt, chim": "Fresh Meat",
    "Thịt sơ chế": "Fresh Meat",
    "Trứng gà, vịt, cút": "Fresh Meat",
    "Cá, hải sản, khô": "Seafood & Fish Balls",
    "Cá hộp": "Instant Foods",
    "Lạp xưởng": "Cold Cuts: Sausages & Ham",
    "Xúc xích": "Cold Cuts: Sausages & Ham",
    "Heo, bò, pate hộp": "Instant Foods",
    "Chả giò, chả ram": "Instant Foods",
    "Chả lụa, thịt nguội": "Cold Cuts: Sausages & Ham",
    "Xúc xích, lạp xưởng tươi": "Cold Cuts: Sausages & Ham",
    "Cá viên, bò viên": "Instant Foods",
    "Thịt, cá đông lạnh": "Instant Foods",

    # Rau, củ, quả, nấm
    "Trái cây": "Fresh Fruits",
    "Rau lá": "Vegetables",
    "Củ, quả": "Vegetables",
    "Nấm các loại": "Vegetables",
    "Rau, củ làm sẵn": "Vegetables",
    "Rau củ đông lạnh": "Vegetables",

    # Đồ ăn chay
    "Đồ chay ăn liền": "Instant Foods",
    "Đậu hũ, đồ chay khác": "Instant Foods",
    "Đậu hũ, tàu hũ": "Instant Foods",

    # Ngũ cốc, tinh bột
    "Ngũ cốc": "Cereals & Grains",
    "Ngũ cốc, yến mạch": "Cereals & Grains",
    "Gạo các loại": "Grains & Staples",
    "Bột các loại": "Grains & Staples",
    "Đậu, nấm, đồ khô": "Grains & Staples",

    # Mì, bún, phở, cháo
    "Mì ăn liền": "Instant Foods",
    "Phở, bún ăn liền": "Instant Foods",
    "Hủ tiếu, miến": "Instant Foods",
    "Miến, hủ tiếu, phở khô": "Instant Foods",
    "Mì Ý, mì trứng": "Instant Foods",
    "Cháo gói, cháo tươi": "Instant Foods",
    "Bún các loại": "Instant Foods",
    "Nui các loại": "Instant Foods",
    "Bánh tráng các loại": "Instant Foods",
    "Bánh phồng, bánh đa": "Instant Foods",
    "Bánh gạo Hàn Quốc": "Cakes",

    # Gia vị, phụ gia, dầu
    "Nước mắm": "Seasonings",
    "Nước tương": "Seasonings",
    "Tương, chao các loại": "Seasonings",
    "Tương ớt - đen, mayonnaise": "Seasonings",
    "Dầu ăn": "Seasonings",
    "Dầu hào, giấm, bơ": "Seasonings",
    "Gia vị nêm sẵn": "Seasonings",
    "Muối": "Seasonings",
    "Hạt nêm, bột ngọt, bột canh": "Seasonings",
    "Tiêu, sa tế, ớt bột": "Seasonings",
    "Bột nghệ, tỏi, hồi, quế,...": "Seasonings",
    "Nước chấm, mắm": "Seasonings",
    "Mật ong, bột nghệ": "Seasonings",

    # Sữa & các sản phẩm từ sữa
    "Sữa tươi": "Milk",
    "Sữa đặc": "Milk",
    "Sữa pha sẵn": "Milk",
    "Sữa hạt, sữa đậu": "Milk",
    "Sữa ca cao, lúa mạch": "Milk",
    "Sữa trái cây, trà sữa": "Milk",
    "Sữa chua ăn": "Yogurt",
    "Sữa chua uống liền": "Yogurt",
    "Bơ sữa, phô mai": "Milk",

    # Đồ uống
    "Bia, nước có cồn": "Alcoholic Beverages",
    "Rượu": "Alcoholic Beverages",
    "Nước trà": "Beverages",
    "Nước ngọt": "Beverages",
    "Nước ép trái cây": "Beverages",
    "Nước yến": "Beverages",
    "Nước tăng lực, bù khoáng": "Beverages",
    "Nước suối": "Beverages",
    "Cà phê hoà tan": "Beverages",
    "Cà phê pha phin": "Beverages",
    "Cà phê lon": "Beverages",
    "Trà khô, túi lọc": "Beverages",

    # Bánh kẹo, snack
    "Bánh tươi, Sandwich": "Cakes",
    "Bánh bông lan": "Cakes",
    "Bánh quy": "Cakes",
    "Bánh snack, rong biển": "Snacks",
    "Bánh Chocopie": "Cakes",
    "Bánh gạo": "Cakes",
    "Bánh quế": "Cakes",
    "Bánh que": "Cakes",
    "Bánh xốp": "Cakes",
    "Kẹo cứng": "Candies",
    "Kẹo dẻo, kẹo marshmallow": "Candies",
    "Kẹo singum": "Candies",
    "Socola": "Candies",
    "Trái cây sấy": "Dried Fruits",
    "Hạt khô": "Dried Fruits",
    "Rong biển các loại": "Snacks",
    "Rau câu, thạch dừa": "Fruit Jam",
    "Mứt trái cây": "Fruit Jam",
    "Cơm cháy, bánh tráng": "Snacks",

    # Món ăn chế biến sẵn, đông lạnh
    "Làm sẵn, ăn liền": "Instant Foods",
    "Sơ chế, tẩm ướp": "Instant Foods",
    "Nước lẩu, viên thả lẩu": "Instant Foods",
    "Kim chi, đồ chua": "Instant Foods",
    "Mandu, há cảo, sủi cảo": "Instant Foods",
    "Bánh bao, bánh mì, pizza": "Instant Foods",
    "Kem cây, kem hộp": "Ice Cream & Cheese",
    "Bánh flan, thạch, chè": "Cakes",
    "Trái cây hộp, siro": "Fruit Jam",

    # Khác
    "Cá mắm, dưa mắm": "Seasonings",
    "Đường": "Seasonings",
    "Nước cốt dừa lon": "Seasonings",
    "Sữa chua uống": "Yogurt",
    "Khô chế biến sẵn": "Instant Foods"
}

# ===== TEXT PROCESSING FUNCTIONS =====
def translate_vi2en(vi_text: str) -> str:
    # """Translate Vietnamese text to English"""
    try:
        inputs = tokenizer_vi2en(vi_text, return_tensors="pt")
        decoder_start_token_id = tokenizer_vi2en.lang_code_to_id["en_XX"]
        outputs = model_vi2en.generate(
            **inputs,
            decoder_start_token_id=decoder_start_token_id,
            num_beams=5,
            early_stopping=True
        )
        return tokenizer_vi2en.decode(outputs[0], skip_special_tokens=True)
    except Exception as e:
        print(f"Translation error for '{vi_text}': {e}")
        return ""

def tokenize_by_whitespace(text: str) -> List[str]:
    # """Tokenize text by whitespace, filter tokens >= 2 chars"""
    if text is None:
        return []
    return [token for token in text.lower().split() if len(token) >= 2]

def generate_ngrams(token: str, n: int) -> List[str]:
    # """Generate n-grams from a single token"""
    if token is None or len(token) < n:
        return []
    return [token[i:i+n] for i in range(len(token) - n + 1)]

def generate_token_ngrams(text: str, n: int) -> List[str]:
    # """Generate n-grams from all tokens in text"""
    tokens = tokenize_by_whitespace(text)
    ngrams = []
    for token in tokens:
        ngrams.extend(generate_ngrams(token, n))
    return ngrams

def parse_store_line(s: str) -> Dict[str, str]:
    # """Parse store location string into name and location"""
    name = s.split('(', 1)[0].strip()

    start = s.find('(')
    if start == -1:
        return {"store_name": name, "store_location": ""}
    
    level = 0
    end = None
    for i, ch in enumerate(s[start:], start):
        if ch == '(':
            level += 1
        elif ch == ')':
            level -= 1
            if level == 0:
                end = i
                break
    
    content = s[start+1:end] if end else ""
    location = re.sub(r'\([^)]*\)', '', content).strip()
    location = location.strip(',').strip()

    return {
        "store_name": name,
        "store_location": location
    }

# ===== PRICE & UNIT PROCESSING =====
def extract_net_value_and_unit_from_name(name: str, fallback_unit: str) -> tuple:
    """Extract net value and unit from product name"""
    tmp_name = name.lower()
    matches = re.findall(r"(\d+(?:\.\d+)?)\s*(g|ml|lít|kg|gói|l)\b", tmp_name)
    if matches:
        value, unit = matches[-1]  # use the LAST match
        return float(value), unit
    return 1, fallback_unit

def normalize_net_value(unit: str, net_value: float, name: str) -> tuple:
    """Normalize net value based on unit and product name"""
    unit = unit.lower()
    name_lower = name.lower()

    # Convert kg to g, lít to ml
    if unit == "kg":
        return float(net_value) * 1000, "g"
    elif unit == "lít":
        return float(net_value) * 1000, "ml"
    
    # Extract kg from name if unit is not standard
    if unit not in ["kg", "g", "ml", "lít"]:
        match_kg = re.search(r"(\d+(\.\d+)?)\s*kg", name_lower)
        if match_kg:
            value = float(match_kg.group(1))
            return value * 1000, unit
    
    # Special case: túi 1kg
    if unit == "túi 1kg":
        return float(net_value) * 1000, "túi"
    
    # Túi with fruit - assume 0.7kg
    if unit == "túi" and "trái" in name_lower:
        return 0.7 * 1000, unit

    # Box/tray with eggs count
    if unit in ["hộp", "vỉ"] and "quả" in name_lower:
        matches = re.findall(rf"{unit}\s*(\d+)", name_lower)
        if matches:
            return sum(map(int, matches)), unit

    # Case/pack with multiple items
    match_pack = re.search(r"(thùng|lốc)\s*(\d+).*?(\d+(\.\d+)?)\s*(g|ml)", name_lower)
    if match_pack:
        count = int(match_pack.group(2))
        per_item = float(match_pack.group(3))
        return count * per_item, unit

    # Fallback: extract from name
    extracted_value, _ = extract_net_value_and_unit_from_name(name, unit)
    if extracted_value > 0:
        return extracted_value, unit

    return float(net_value) if net_value != 0 else 1, unit

def extract_best_price(product: dict) -> dict:
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
    
    # Priority 1: Campaign pricing
    if campaign_info:
        campaign = campaign_info[0]
        campaign_price = campaign.get("productPrice", {})
        net_value = campaign_price.get("netUnitValue", 0)
        net_value, converted_unit = normalize_net_value(original_unit, net_value, name)
        return build_result(campaign_price, converted_unit, net_value)

    # Priority 2: Base pricing
    if base_price_info:
        price_info = base_price_info[0]
        net_value = price_info.get("netUnitValue", 0)
        net_value, converted_unit = normalize_net_value(original_unit, net_value, name)
        return build_result(price_info, converted_unit, net_value)

    # Fallback: No pricing info
    return {
        "name": name,
        "unit": original_unit,
        "netUnitValue": 1,
        "price": None,
        "sysPrice": None,
        "discountPercent": None,
        "date_begin": None,
        "date_end": None,
    }

# ===== PRODUCT DATA PROCESSING =====
def process_product_data(product: dict, category_name: str, store_id: int) -> dict:
    """Process raw product data - SKIP if already exists in DB"""
    from db import MongoDB
    db = MongoDB.get_db()
    
    sku = product.get("id")
    if not sku:
        raise ValueError("Product missing SKU")
    
    # Check if product already exists for this store
    coll_name = category_name.replace(" ", "_").replace("&", "and").replace(":", "").lower()
    existing = db[coll_name].find_one({"sku": sku, "store_id": store_id})
    
    if existing:
        print(f"⏭️ SKU {sku} already exists for store {store_id} - skipping processing")
        return None  # Skip toàn bộ processing
    
    # Chỉ process khi chưa tồn tại
    print(f"🔄 Processing new product: SKU {sku} for store {store_id}")
    
    # Translate name (chỉ khi cần thiết)
    english_name = translate_vi2en(product.get("name", ""))
    if not english_name:
        raise ValueError(f"Failed to translate name: {product.get('name', '')}")
    
    # Extract price information
    price_info = extract_best_price(product)
    
    # Generate search tokens
    token_ngrams = generate_token_ngrams(english_name, 2)
    
    # Build standardized product data
    return {
        "sku": sku,
        "name": product["name"],
        "name_en": english_name,
        "unit": price_info["unit"].lower(),
        "netUnitValue": price_info["netUnitValue"], 
        "token_ngrams": token_ngrams,
        "category": category_name, 
        "store_id": store_id, 
        "url": f"https://www.bachhoaxanh.com{product['url']}",
        "image": product["avatar"],
        "promotion": product.get("promotionText", ""),
        "price": price_info["price"],
        "sysPrice": price_info["sysPrice"],
        "discountPercent": price_info["discountPercent"],
        "date_begin": price_info["date_begin"],
        "date_end": price_info["date_end"],
        "crawled_at": datetime.utcnow().isoformat(),
    }

# ===== DATABASE OPERATIONS =====
async def upsert_product(product_data: dict, category_title: str, db):
    """Upsert single product to MongoDB"""
    coll_name = category_title.replace(" ", "_").lower()
    product_db = db[coll_name]

    sku = product_data.get("sku")
    if not sku:
        print("No SKU found for product, skipping upsert.")
        return

    product_db.update_one(
        {"sku": sku},
        {"$set": product_data},
        upsert=True
    )
    print(f"✓ Upserted product: {product_data.get('name', '')} "
          f"(SKU: {sku}) into collection: {coll_name}")

async def upsert_products_bulk(product_list: List[dict], category_title: str, db):
    """Bulk upsert products to MongoDB"""
    coll_name = category_title.replace(" ", "_").lower()
    collection = db[coll_name]
    ops = []
    
    for p in product_list:
        sku = p.get("sku")
        if not sku:
            continue
        ops.append(
            UpdateOne(
                {"sku": sku},
                {"$set": p},
                upsert=True
            )
        )
    
    if not ops:
        return
    
    result = collection.bulk_write(ops, ordered=False)
    print(f"✓ Bulk upserted {len(ops)} products into `{coll_name}` "
          f"(upserted: {result.upserted_count}, modified: {result.modified_count})")

def reset_category_collections(db):
    """Drop all category collections from database"""
    try:
        for cat_doc in db.categorys.find({}, {"name": 1}):
            coll_name = cat_doc["name"].lower().replace(" ", "_")
            if coll_name in db.list_collection_names():
                print(f"Dropping collection: {coll_name}")
                db.drop_collection(coll_name)
    except Exception as e:
        print(f"Error resetting collections: {e}")
        return
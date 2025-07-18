import hashlib
import re
from typing import List, Dict
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
from datetime import datetime
from pymongo import UpdateOne
from tqdm import tqdm


# ===== TRANSLATION SETUP =====
tokenizer_vi2en = AutoTokenizer.from_pretrained(
    "vinai/vinai-translate-vi2en-v2",
    use_fast=False,
    src_lang="vi_VN",
    tgt_lang="en_XX"
)
model_vi2en = AutoModelForSeq2SeqLM.from_pretrained("vinai/vinai-translate-vi2en-v2")

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
    if not text: return []
    return [t for t in text.lower().split() if len(t)>=2]

def generate_token_ngrams(text: str, n: int) -> List[str]:
    tokens = tokenize_by_whitespace(text)
    ngrams = []
    for t in tokens:
        ngrams += [t[i:i+n] for i in range(len(t)-n+1)]
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
        value, unit = matches[-1]
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

def process_unit_and_net_value(product: dict) -> dict:
    """
    Xử lý phần unit và netUnitValue từ dữ liệu product,
    trả về dict gồm unit và netUnitValue đã chuẩn hóa.
    """
    name = product.get("name", "")
    original_unit = product.get("unit", "").lower()
    net_unit_value = product.get("netUnitValue", 0)
    
    nv, u = normalize_net_value(original_unit, net_unit_value, name)
    
    return {
        "unit": u,
        "netUnitValue": nv
    }

def extract_best_price(product: dict) -> dict:
    """
    Chỉ lấy thông tin giá, khuyến mãi, ngày bắt đầu/kết thúc,
    không xử lý unit hay netUnitValue ở đây.
    """
    base_price_info = product.get("productPrices", [])
    campaign_info = product.get("lstCampaingInfo", [])
    
    def build_result(info: dict):
        return {
            "price": info.get("price"),
            "sysPrice": info.get("sysPrice"),
            "discountPercent": info.get("discountPercent"),
            "date_begin": info.get("startTime") or info.get("poDate"),
            "date_end": info.get("dueTime") or info.get("poDate"),
        }
    
    # Ưu tiên giá khuyến mãi
    if campaign_info:
        return build_result(campaign_info[0].get("productPrice", {}))
    
    # Giá gốc
    if base_price_info:
        return build_result(base_price_info[0])
    
    # Không có thông tin giá
    return {
        "price": None,
        "sysPrice": None,
        "discountPercent": None,
        "date_begin": None,
        "date_end": None,
    }


def fingerprint(p: dict) -> str:
    s = f"{p['sku']}|{p['price']}|{p['discountPercent']}"
    return hashlib.md5(s.encode()).hexdigest()

async def process_product_data(raw: List[dict], category: str, store_id: int, db) -> List[UpdateOne]:
    coll = db[category.replace(" ","_").lower()]
    ops = []
    for prod in raw:
        sku = prod.get("id")
        if not sku:
            continue
        
        filt = {"sku": sku, "store_id": store_id}
        exist = await coll.find_one(filt, {"price": 1, "hash": 1})
        
        price_info = extract_best_price(prod)
        
        if exist:
            if exist.get("price") == price_info["price"]:
                continue
            upd = {
                **price_info,
                "crawled_at": datetime.utcnow().isoformat()
            }
        else:
            # Xử lý dịch tên, token ngram
            name = prod.get("name", "")
            name_en = translate_vi2en(name)
            ngram = generate_token_ngrams(name_en, 2)
            
            # Xử lý unit và netUnitValue riêng
            unit_info = process_unit_and_net_value(prod)
            
            upd = {
                "sku": sku,
                "name": name,
                "name_en": name_en,
                "token_ngrams": ngram,
                **unit_info,
                **price_info,
                "category": category,
                "store_id": store_id,
                "url": f"https://www.bachhoaxanh.com{prod.get('url', '')}",
                "image": prod.get("avatar", ""),
                "promotion": prod.get("promotionText", ""),
                "crawled_at": datetime.utcnow().isoformat(),
            }
            upd["hash"] = fingerprint({**upd})
        
        ops.append(UpdateOne(filt, {"$set": upd}, upsert=True))
    return ops



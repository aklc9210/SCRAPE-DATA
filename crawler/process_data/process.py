import re
from typing import List, Dict, Tuple
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM

CATEGORIES_MAPPING_BHX = {
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

CATEGORIES_MAPPING_WINMART = {
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


# list các unit cơ bản
BASE_UNITS = ["g", "ml", "lít", "kg", "l"]

# list các từ đóng gói
PACK_UNITS = ["lốc", "thùng", "combo", "set", "chai", "lon", "hũ"]

# build pattern động
unit_pattern = "|".join(re.escape(u) for u in BASE_UNITS)
pack_pattern = "|".join(re.escape(p) for p in PACK_UNITS)


import torch
torch.set_num_threads(4)
# ===== TRANSLATION SETUP =====
tokenizer_vi2en = AutoTokenizer.from_pretrained(
    "vinai/vinai-translate-vi2en-v2",
    use_fast=False,
    src_lang="vi_VN",
    tgt_lang="en_XX"
)
model_vi2en = AutoModelForSeq2SeqLM.from_pretrained("vinai/vinai-translate-vi2en-v2")


# ===== TEXT PROCESSING FUNCTIONS =====
async def translate_vi2en(vi_text: str) -> str:
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
        # print(f"Translation error for '{vi_text}': {e}")
        return ""

async def tokenize_by_whitespace(text: str) -> List[str]:
    if not text: return []
    return [t for t in text.lower().split() if len(t)>=2]

async def generate_token_ngrams(text: str, n: int) -> List[str]:
    tokens = await tokenize_by_whitespace(text)
    ngrams = []
    for t in tokens:
        ngrams += [t[i:i+n] for i in range(len(t)-n+1)]
    return ngrams

async def parse_store_line(s: str) -> Dict[str, str]:
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
async def extract_net_value_and_unit_from_name(name: str, fallback_unit: str) -> tuple:
    """Extract net value and unit from product name"""
    tmp_name = name.lower()

    # Trường hợp "X x Yml", "Yml x X", "3 x 250ml", "250ml x 3"
    match_combo = re.search(r"(\d+)\s*[x×*]\s*(\d+(?:\.\d+)?)\s*(ml|g|kg|l|lít)", tmp_name) or \
                  re.search(r"(\d+(?:\.\d+)?)\s*(ml|g|kg|l|lít)\s*[x×*]\s*(\d+)", tmp_name)
    if match_combo:
        if match_combo.lastindex == 3:
            count = int(match_combo.group(1))
            value = float(match_combo.group(2))
            unit = match_combo.group(3)
        else:
            value = float(match_combo.group(1))
            unit = match_combo.group(2)
            count = int(match_combo.group(3))

        # Chuẩn hóa
        if unit in ["kg"]:
            return count * value * 1000, "g"
        elif unit in ["l", "lít"]:
            return count * value * 1000, "ml"
        return count * value, unit

    # Trường hợp bình thường
    matches = re.findall(r"(\d+(?:\.\d+)?)\s*(g|ml|lít|kg|gói|l)\b", tmp_name)
    if matches:
        value, unit = matches[-1]
        return float(value), unit
    return 1, fallback_unit

async def normalize_net_value(unit: str, net_value: float, name: str) -> Tuple[float, str]:
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

    # thùng/lốc X items of Y ml/g each (thùng 24 lon 330ml)
    match_pack = re.search(r"(thùng|lốc)\s*(\d+).*?(\d+(?:\.\d+)?)\s*(g|ml)", name_lower)
    if match_pack:
        count = int(match_pack.group(2))
        per_item = float(match_pack.group(3))
        return count * per_item, unit

    # trường hợp Y ml/g trước, từ đóng gói (lốc/thùng/…) sau
    pattern = re.compile(
        rf"(\d+(?:\.\d+)?)\s*({unit_pattern})"    
        rf"[^\d]*"                               
        rf"(?:{pack_pattern})?\s*"               
        rf"(\d+)",                              
        re.IGNORECASE
    )
    match_reverse_pack = pattern.search(name_lower)
    if match_reverse_pack:
        per_unit   = float(match_reverse_pack.group(1))
        unit_detected = match_reverse_pack.group(2)
        count      = int(match_reverse_pack.group(3))

        # normalize kg → g, lít → ml
        if unit_detected == "kg":
            return per_unit * count * 1000, "g"
        elif unit_detected in ["l", "lít"]:
            return per_unit * count * 1000, "ml"
        return per_unit * count, unit_detected

    # Fallback: extract from name
    extracted_value, extracted_unit = await extract_net_value_and_unit_from_name(name, unit)
    if extracted_value > 0:
        return extracted_value, extracted_unit

    return float(net_value) if net_value != 0 else 1.0, unit

async def process_unit_and_net_value(product: dict) -> dict:
    """
    Xử lý phần unit và netUnitValue từ dữ liệu product,
    trả về dict gồm unit và netUnitValue đã chuẩn hóa.
    """
    name = product.get("name", "")
    original_unit = product.get("unit", "").lower()
    net_unit_value = product.get("netUnitValue", 0)
    
    nv, u = await normalize_net_value(original_unit, net_unit_value, name)
    
    return {
        "unit": u,
        "net_unit_value": nv
    }
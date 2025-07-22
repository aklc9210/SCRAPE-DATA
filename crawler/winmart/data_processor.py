import re
from datetime import datetime
import torch
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM

# Mapping từ tiếng Việt sang tiếng Anh cho danh mục sản phẩm
CATEGORIES_MAPPING = {
    "Sữa các loại": "Milk",
    "Sữa Tươi": "Milk",
    "Sữa Hạt - Sữa Đậu": "Milk",
    "Sữa Bột": "Milk",
    "Bơ Sữa - Phô Mai": "Milk",
    "Sữa đặc": "Milk",
    "Sữa Chua - Váng Sữa": "Yogurt",
    "Sữa Bột - Sữa Dinh Dưỡng": "Milk",

    "Rau - Củ - Trái Cây": "Vegetables",
    "Rau Lá": "Vegetables",
    "Củ, Quả": "Vegetables",
    "Trái cây tươi": "Fresh Fruits",

    "Thịt - Hải Sản Tươi": "Fresh Meat",
    "Thịt": "Fresh Meat",
    "Hải Sản": "Seafood & Fish Balls",
    "Trứng - Đậu Hũ": "Fresh Meat",
    "Trứng": "Fresh Meat",
    "Đậu hũ": "Instant Foods",
    "Thịt Đông Lạnh": "Instant Foods",
    "Hải Sản Đông Lạnh": "Instant Foods",

    "Bánh Kẹo": "Cakes",
    "Bánh Xốp - Bánh Quy": "Cakes",
    "Kẹo - Chocolate": "Candies",
    "Bánh Snack": "Snacks",
    "Hạt - Trái Cây Sấy Khô": "Dried Fruits",

    "Đồ uống có cồn": "Alcoholic Beverages",
    "Bia": "Alcoholic Beverages",
    "Đồ Uống - Giải Khát": "Beverages",
    "Cà Phê": "Beverages",
    "Nước Suối": "Beverages",
    "Nước Ngọt": "Beverages",
    "Trà - Các Loại Khác": "Beverages",

    "Mì - Thực Phẩm Ăn Liền": "Instant Foods",
    "Mì": "Instant Foods",
    "Miến - Hủ Tíu - Bánh Canh": "Instant Foods",
    "Cháo": "Instant Foods",
    "Phở - Bún": "Instant Foods",

    "Thực Phẩm Khô": "Grains & Staples",
    "Gạo - Nông Sản Khô": "Grains & Staples",
    "Ngũ Cốc - Yến Mạch": "Cereals & Grains",
    "Thực Phẩm Đóng Hộp": "Instant Foods",
    "Rong Biển - Tảo Biển": "Snacks",
    "Bột Các Loại": "Grains & Staples",
    "Thực Phẩm Chay": "Instant Foods",

    "Thực Phẩm Chế Biến": "Instant Foods",
    "Bánh mì": "Instant Foods",
    "Xúc xích - Thịt Nguội": "Cold Cuts: Sausages & Ham",
    "Bánh bao": "Instant Foods",
    "Kim chi": "Instant Foods",
    "Thực Phẩm Chế Biến Khác": "Instant Foods",

    "Gia vị": "Seasonings",
    "Dầu Ăn": "Seasonings",
    "Nước Mắm - Nước Chấm": "Seasonings",
    "Đường": "Seasonings",
    "Nước Tương": "Seasonings",
    "Hạt Nêm": "Seasonings",
    "Tương Các Loại": "Seasonings",
    "Gia Vị Khác": "Seasonings",

    "Thực Phẩm Đông Lạnh": "Instant Foods",
    "Chả Giò": "Instant Foods",
    "Cá - Bò Viên": "Instant Foods",
    "Thực Phẩm Đông Lạnh Khác": "Instant Foods"
}

torch.set_num_threads(4)
tokenizer = AutoTokenizer.from_pretrained(
    "vinai/vinai-translate-vi2en-v2",
    use_fast=False,
    src_lang="vi_VN",
    tgt_lang="en_XX",
    legacy=False
)
model = AutoModelForSeq2SeqLM.from_pretrained(
    "vinai/vinai-translate-vi2en-v2"
)

async def translate_vi2en(text: str) -> str:
    """
    Dịch tên sản phẩm từ tiếng Việt sang tiếng Anh sử dụng mô hình Transformer.
    """
    try:
        inputs = tokenizer(text, return_tensors="pt")
        decoder_start_token_id = tokenizer.lang_code_to_id["en_XX"]
        outputs = model.generate(
            **inputs,
            decoder_start_token_id=decoder_start_token_id,
            num_beams=5,
            early_stopping=True
        )
        return tokenizer.decode(outputs[0], skip_special_tokens=True)
    except Exception as e:
        print(f"Translation error for '{text}': {e}")
        return ""

async def generate_token_ngrams(text: str, n: int) -> list:
    """
    Sinh danh sách token n-grams từ chuỗi tiếng Anh đã dịch.
    """
    tokens = text.lower().split()
    if len(tokens) < n:
        return tokens
    return [" ".join(tokens[i:i+n]) for i in range(len(tokens) - n + 1)]

async def normalize_net_value(unit: str, net_value: float, name: str) -> tuple:
    """
    Chuẩn hoá đơn vị và giá trị (kg->g, l->ml, giữ nguyên đơn vị tính đếm).
    """
    unit_lower = (unit or "").strip().lower()
    # Trọng lượng
    if "kg" in unit_lower:
        return net_value * 1000, "g"
    if "g" in unit_lower and "kg" not in unit_lower:
        return net_value, "g"
    # Thể tích
    if "l" in unit_lower and "ml" not in unit_lower:
        return net_value * 1000, "ml"
    if "ml" in unit_lower:
        return net_value, "ml"
    # Đơn vị đếm
    for u in ["cái", "chiếc", "gói", "hộp", "túi", "bịch"]:
        if u in unit_lower:
            return net_value, u
    # Thử parse từ tên sản phẩm nếu có pattern số + đơn vị
    m = re.search(r"(\d+(?:[\.,]?\d*))(kg|g|l|ml)", name.lower())
    if m:
        val, u = m.groups()
        val = float(val.replace(',', '.'))
        if u == "kg":
            return val * 1000, "g"
        if u == "l":
            return val * 1000, "ml"
        return val, u
    # Fallback: giữ nguyên
    return net_value, unit_lower or "unit"

async def process_product(product: dict, db) -> dict:
    """
    Chuyển một raw product dict thành record chuẩn để upsert,
    tái sử dụng translation/ngrams nếu đã có bản ghi cùng tên.
    """
    name = product.get("name", "").strip()
    if not name:
        return None

    # Xác định collection name từ mapped category
    coll_name = product.get("mapped_category", "").replace(" ", "_").lower()
    coll = db[coll_name]

    try:
        # Kiểm tra reuse translation/ngrams
        trans_doc = await coll.find_one(
            {"name": name},
            {"name_en": 1, "token_ngrams": 1}
        )
    except Exception as e:
        print("‼️ Error during find_one:", e)
        raise

    if trans_doc:
        name_en = trans_doc["name_en"]
        token_ngrams = trans_doc["token_ngrams"]
    else:
        # Dịch và sinh ngrams mới
        name_en = await translate_vi2en(name)
        if not name_en:
            return None
        token_ngrams = await generate_token_ngrams(name_en, 2)

    # Giá gốc, giá sale và discount percent
    orig = float(product.get("original_price", product.get("price", 0)))
    sale = float(product.get("sale_price", 0))
    discount_percent = ((orig - sale) / orig * 100) if orig > 0 else 0.0

    # Chuẩn hoá unit và net_value
    norm_val, norm_unit = await normalize_net_value(
        product.get("uom", "piece"),
        float(product.get("quantity_per_unit", 1)),
        name
    )

    # Xây dựng record
    record = {
        "sku": product.get("product_id", "") or product.get("sku", ""),
        "name": name,
        "name_en": name_en,
        "token_ngrams": token_ngrams,
        "unit": norm_unit.lower(),
        "net_unit_value": norm_val,
        "category": product.get("mapped_category"),
        "store_id": product.get("store_id"),
        "url": product.get("media_url") or product.get("image"),
        "image": product.get("media_url") or product.get("image"),
        "promotion": product.get("promotion_text"),
        "price": sale if sale else orig,
        "sys_price": orig,
        "discount_percent": round(discount_percent, 2),
        "date_begin": datetime.now().strftime("%Y-%m-%d"),
        "date_end": datetime.now().strftime("%Y-%m-%d"),
        "chain": "WM",
        "crawled_at": datetime.utcnow().isoformat()
    }
    return record

async def process_products_batch(products: list, db) -> list:
    """
    Chạy process_product cho danh sách sản phẩm thô và trả về list bản ghi đã chuẩn hoá.
    """
    result = []
    for p in products:
        try:
            rec = await process_product(p, db)
            if rec:
                result.append(rec)
        except Exception:
            continue
    return result

import re
from db.db_async import db
from datetime import datetime
import torch
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM

class DataProcessor:
    """
    Xử lý sản phẩm WinMart: dịch tên, sinh token-ngrams, chuẩn hoá đơn vị, tính giá và build record.
    """
    def __init__(self):
        torch.set_num_threads(4)
        self.tokenizer = AutoTokenizer.from_pretrained(
            "vinai/vinai-translate-vi2en-v2",
            use_fast=False,
            src_lang="vi_VN",
            tgt_lang="en_XX",
            legacy=False
        )
        self.model = AutoModelForSeq2SeqLM.from_pretrained(
            "vinai/vinai-translate-vi2en-v2"
        )

    async def translate_vi2en(self, text: str) -> str:
        """
        Dịch một chuỗi tiếng Việt sang tiếng Anh, trả về chuỗi kết quả.
        """
        if not text:
            return ""
        inputs = self.tokenizer(text, return_tensors="pt")
        outputs = self.model.generate(**inputs, max_length=512)
        translation = self.tokenizer.decode(outputs[0], skip_special_tokens=True)
        return translation.strip()

    async def generate_token_ngrams(self, text: str, n: int) -> list:
        """
        Sinh danh sách n-grams từ chuỗi tiếng Anh đã dịch.
        """
        tokens = text.lower().split()
        if len(tokens) < n:
            return tokens
        return [" ".join(tokens[i:i+n]) for i in range(len(tokens) - n + 1)]

    async def normalize_net_value(self, unit: str, net_value: float, name: str) -> tuple:
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

    async def process_product(self, product: dict) -> dict:
        """
        Chuyển một raw product dict thành record chuẩn để upsert,
        tái sử dụng translation/ngrams nếu đã có bản ghi cùng name.
        """
        name = product.get("name", "").strip()
        if not name:
            return None

        # Xác định collection
        coll_name = product.get("mapped_category", "").replace(" ", "_").lower()
        coll = db[coll_name]

        # Kiểm tra reuse translation/ngrams
        trans_doc = await coll.find_one(
            {"name": name},
            {"name_en": 1, "token_ngrams": 1}
        )
        if trans_doc:
            name_en = trans_doc["name_en"]
            token_ngrams = trans_doc["token_ngrams"]
        else:
            # Dịch và sinh ngrams mới
            name_en = await self.translate_vi2en(name)
            if not name_en:
                return None
            token_ngrams = await self.generate_token_ngrams(name_en, 2)

        # Giá gốc, giá sale và discount percent
        orig = float(product.get("original_price", product.get("price", 0)))
        sale = float(product.get("sale_price", 0))
        discount_percent = ((orig - sale) / orig * 100) if orig > 0 else 0.0

        # Chuẩn hoá unit và net_value
        norm_val, norm_unit = await self.normalize_net_value(
            product.get("uom", "piece"),
            float(product.get("quantity_per_unit", 1)),
            name
        )

        # Build kết quả record
        record = {
            "sku": product.get("sku") or product.get("product_id"),
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

    async def process_products_batch(self, products: list) -> list:
        """
        Chạy `process_product` cho danh sách raw và trả về list record đã normalize.
        """
        result = []
        for p in products:
            try:
                rec = await self.process_product(p)
                if rec:
                    result.append(rec)
            except Exception:
                continue
        return result
    
    

from datetime import datetime
import re
from crawler.process_data.process import translate_vi2en, generate_token_ngrams, normalize_net_value

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
        # print("‼️ Error during find_one:", e)
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

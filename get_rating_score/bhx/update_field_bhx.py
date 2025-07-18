import json
from bson import ObjectId  # 👈 import cần thiết
from db import MongoDB

db = MongoDB.get_db()
collection = db.stores

# Đọc dữ liệu đã merge
with open("merged_bhx_data.json", "r", encoding="utf-8") as f:
    merged_data = json.load(f)

# Cập nhật từng bản ghi dựa vào _id
for store in merged_data:
    _id_dict = store.get("_id")
    if not _id_dict or "$oid" not in _id_dict:
        continue

    _id = ObjectId(_id_dict["$oid"])  # ✅ chuyển thành ObjectId thật

    # Tạo dữ liệu cần cập nhật
    update_fields = {
        "imageUrl": store.get("imageUrl"),
        "totalScore": store.get("totalScore"),
        "reviewsCount": store.get("reviewsCount"),
        "phone": store.get("phone"),
        "url": store.get("url")
    }

    # Cập nhật vào MongoDB
    collection.update_one(
        {"_id": _id, "chain": "BHX"},
        {"$set": update_fields},
        upsert=False
    )

print(f"Đã cập nhật {len(merged_data)} cửa hàng vào MongoDB.")

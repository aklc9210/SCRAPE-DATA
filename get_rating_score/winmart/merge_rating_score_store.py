import json
import re
from difflib import SequenceMatcher

# Đường dẫn tới file
hcm_path = "json/filtered_data_hcm.json"
mongo_path = "../store_recommender.stores.json"
output_path = "json/merged_winmart_data.json"

# Đọc dữ liệu từ hai file JSON
with open(hcm_path, "r", encoding="utf-8") as f1, open(mongo_path, "r", encoding="utf-8") as f2:
    hcm_data = json.load(f1)
    mongo_data = json.load(f2)

# Hàm chuẩn hóa tên
def normalize_title(title: str) -> str:
    title = title.lower()
    # Loại bỏ các cụm từ phổ biến trong tên cửa hàng Winmart
    title = re.sub(r"(siêu thị|cửa hàng|ch|winmart\+?|vinmart\+?|win\+?|wm\+?|wm|win)", "", title)
    # Loại bỏ ký tự đặc biệt và khoảng trắng thừa
    title = re.sub(r"[^a-zA-Z0-9\s]", "", title)
    title = re.sub(r"\s+", " ", title).strip()
    return title


# Tạo dict từ dữ liệu Google Maps
hcm_dict = {
    normalize_title(item["title"]): item
    for item in hcm_data
}

# Hợp nhất dữ liệu theo tên gần giống
merged_data = []
for store in mongo_data:
    if store.get("chain", "").strip().lower() != "winmart" and store.get("chain", "").strip().lower() != "winmart+":
        continue

    norm_store_name = normalize_title(store.get("name", ""))
    best_match = None
    best_ratio = 0

    for hcm_title, hcm_item in hcm_dict.items():
        ratio = SequenceMatcher(None, norm_store_name, hcm_title).ratio()
        if ratio > best_ratio:
            best_match = hcm_item
            best_ratio = ratio

    # Nếu tên đủ giống thì hợp nhất
    if best_match and best_ratio > 0.5:
        merged = store.copy()
        merged.update({
            "imageUrl": best_match.get("imageUrl"),
            "totalScore": best_match.get("totalScore"),
            "reviewsCount": best_match.get("reviewsCount"),
            "phone": best_match.get("phone"),
            "url": best_match.get("url"),
        })
        merged_data.append(merged)

# Ghi ra file kết quả
with open(output_path, "w", encoding="utf-8") as f:
    json.dump(merged_data, f, ensure_ascii=False, indent=2)

print(f"Đã lưu {len(merged_data)} bản ghi vào: {output_path}")

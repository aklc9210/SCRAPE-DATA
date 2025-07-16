import json

# Đường dẫn tới file JSON gốc
input_path = "dataset_google-maps-extractor_2025-07-16_02-47-30-347.json"

# Đọc dữ liệu từ file
with open(input_path, "r", encoding="utf-8") as f:
    data = json.load(f)

# Lọc và loại bỏ các trường không cần thiết
filtered_data = []
for item in data:
    if item.get("city", "").strip().lower() == "hồ chí minh":
        # Xóa các trường không cần
        item.pop("categoryName", None)
        item.pop("countryCode", None)
        item.pop("website", None)
        filtered_data.append(item)

# Ghi dữ liệu đã lọc ra file mới
output_path = "filtered_data_hcm.json"
with open(output_path, "w", encoding="utf-8") as f:
    json.dump(filtered_data, f, ensure_ascii=False, indent=2)

print(f"Đã lưu file đã lọc: {output_path}")
print(f"Sô lượng store đã lấy {len(filtered_data)}")

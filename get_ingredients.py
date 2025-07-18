from pymongo import MongoClient
from bson import ObjectId
import json
from db import MongoDB

db = MongoDB.get_db()

store_id = 3786 

print(store_id)

# Lấy danh sách categories
categories = db.categories.distinct("name")

print(categories)

# Hàm chuyển tên category thành tên collection
def category_to_collection(name):
    return name.lower().replace(" ", "_")

results = []

for cat in categories:
    coll_name = category_to_collection(cat)
    collection = db.get_collection(coll_name)
    if (coll_name == "seafood_&_fish_balls"):
        store_id = 3271

    elif (coll_name == "ice_cream_&_cheese" or coll_name == "grains_&_staples"):
        store_id = 2854

    elif (coll_name == "cereals_&_grains" or coll_name == "cold_cuts:_sausages_&_ham"):
        store_id = 2858
    
    else: store_id = 3786
    
    print(f"Query {coll_name} với store_id={store_id}")

    # Lấy sản phẩm của store_id này
    cursor = collection.find(
        { "store_id": store_id },
        { "_id": 1, "category": 1, "image": 1, "name": 1, "name_en": 1, "unit": 1}
    )

    print(f"Tìm thấy {collection.count_documents({'store_id': store_id})} sản phẩm")
    for doc in cursor:
        # đảm bảo _id là str
        doc["_id"] = str(doc["_id"])
        results.append(doc)

# Lưu ra file JSON
with open("products_by_store.json", "w", encoding="utf-8") as f:
    json.dump(results, f, ensure_ascii=False, indent=2)

print(f"Đã lưu {len(results)} sản phẩm vào products_by_store.json")

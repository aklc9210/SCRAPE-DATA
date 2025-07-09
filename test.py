# import pandas as pd

# # Đọc file CSV (thay 'your_file.csv' bằng đường dẫn tới file của bạn)
# df = pd.read_csv('categories.csv')

# # Đếm số giá trị unique trong cột 'category'
# n_unique = df['name'].nunique()

# print(f"Số giá trị unique trong cột 'category': {n_unique}")

# df1 = pd.read_csv('provinces.csv')

# print(f"Số tỉnh thành: {df1['name'].unique()}, {df1['name'].nunique()}, {df1['id'].max()}, {df1['id'].min()}")

# print ("Store")

from typing import Counter
from pymongo import MongoClient

client = MongoClient("mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000")

db = client.store_recommender

chain_db = db.chain

def duplicates(lst):
    return [item for item, cnt in Counter(lst).items() if cnt > 1]

# XÓA PHẦN TỬ KHỎI DB
# chain_db.delete
# chain_db.(
#                     {"_id": 1},
#                     {"$set": {"name": 'Phu Yen', "district": ['Tuy Hoa', 'Dong Hoa', 'Song Cau']}},
#                     upsert=True
#                 )

result = chain_db.delete_one({"_id": 1})

# Get max and min id in province 
max_id = chain_db.find_one(sort=[("_id", -1)])["_id"]
min_id = chain_db.find_one(sort=[("_id", 1)])["_id"]
print(f"Max ID: {max_id}, Min ID: {min_id}")

# Get max and min id in district
for district in chain_db.find({"district": {"$exists": True}}):
    max_id_district = max(district["district"], key=lambda x: x["id"])["id"]
    min_id_district = min(district["district"], key=lambda x: x["id"])["id"]

    print (district)

    if "wards" in district:
        for ward in district["wards"]:
            max_id_ward = max(ward["wards"], key=lambda x: x["id"])["id"]
            min_id_ward = min(ward["wards"], key=lambda x: x["id"])["id"]

print(f"Max ID District: {max_id_district}, Min ID District: {min_id_district}")

# Get max and min id in ward
# max_id_ward = chain_db.find_one({"district.wards": {"$exists": True}}, sort=[("district.wards.id", -1)])["district"][0]["wards"][0]["id"]
# min_id_ward = chain_db.find_one({"district.wards": {"$exists": True}}, sort=[("district.wards.id", 1)])["district"][0]["wards"][0]["id"]
print(f"Max ID Ward: {max_id_ward}, Min ID Ward: {min_id_ward}")

# for name in db.chain.find():
#     print(name)
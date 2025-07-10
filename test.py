# import pandas as pd

# # Đọc file CSV (thay 'your_file.csv' bằng đường dẫn tới file của bạn)
# df = pd.read_csv('categories.csv')

# # Đếm số giá trị unique trong cột 'category'
# n_unique = df['name'].nunique()

# print(f"Số giá trị unique trong cột 'category': {n_unique}")

# df1 = pd.read_csv('provinces.csv')

# print(f"Số tỉnh thành: {df1['name'].unique()}, {df1['name'].nunique()}, {df1['id'].max()}, {df1['id'].min()}")

# print ("Store")

# from typing import Counter
# from pymongo import MongoClient

# client = MongoClient("mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000")

# db = client.store_recommender

# chain_db = db.chain

# def duplicates(lst):
#     return [item for item, cnt in Counter(lst).items() if cnt > 1]

# XÓA PHẦN TỬ KHỎI DB
# chain_db.delete
# chain_db.(
#                     {"_id": 1},
#                     {"$set": {"name": 'Phu Yen', "district": ['Tuy Hoa', 'Dong Hoa', 'Song Cau']}},
#                     upsert=True
#                 )

# result = chain_db.delete_one({"_id": 1})

# for name in db.chain.find():
#     print(name)

import asyncio
from crawler.bhx.test import test_product
if __name__ == "__main__":
    asyncio.run(test_product()) 
    
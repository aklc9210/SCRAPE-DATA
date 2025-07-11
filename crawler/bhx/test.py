
# import time
# import asyncio
# from crawler.bhx.fetch_data import BHXDataFetcher, upsert_products_bulk
# from db import MongoDB

# # --- Thêm hàm đo hiệu năng crawl products cho category "Vegetables" tại TPHCM ---
# async def test_product():
#     """
#     Measure the time to fetch all products in the 'Vegetables' category
#     across all Bach Hoa Xanh stores in Ho Chi Minh City (province_id=3).
#     """
#     db = MongoDB.get_db()
#     cat_doc = db.category.find_one({"name": "Vegetables"})
#     links = cat_doc.get("links", []) if cat_doc else []
#     if not links:
#         print("No 'Vegetables' category found in DB.")
#         return

#     fetcher = BHXDataFetcher()
#     await fetcher.init_token()         

#     # Lấy stores ở TPHCM (province_id=3)
#     stores = db.store.find({ "province_id": 3 })

#     print("Store in TPHCM", stores)

#     start = time.perf_counter()
#     total = 0
#     all_products = []
#     for s in stores:
#         print("Store: ", s['store_location'])
#         pid = 3
#         wid = s.get("ward_id", 0)
#         did = s.get("district_id", 0)
#         sid = s["store_id"]
#         for url in links:
#             prods = await fetcher.fetch_product_info(
#                 province_id=pid,
#                 ward_id=wid,
#                 district_id=did,
#                 store_id=sid,
#                 category_url=url,
#                 isMobile=True,
#                 page_size=10
#             )
#             all_products.extend(prods)
#             total = len(prods)  
#         await upsert_products_bulk(all_products, "Vegetables")

#     elapsed = time.perf_counter() - start
#     print(f"Fetched {total} products for 'Vegetables' in HCMC "
#         f"across stores in {elapsed:.2f}s.")

#     await fetcher.close()
# # --- Kết thúc hàm test_product ---

# if __name__ == "__main__":
    
#     asyncio.run(test_product()) 


from db import MongoDB
 
db = MongoDB.get_db()

def reset_category_collections():
    """
    Xóa (drop) hết tất cả các collection tương ứng với
    các category đã lưu trong collection `category`.
    """
    for cat_doc in db.category.find({}, {"name": 1}):
        # chuyển tên category thành tên collection (ví dụ: "Fresh Meat" -> "fresh_meat")
        coll_name = cat_doc["name"].lower().replace(" ", "_")
        if coll_name in db.list_collection_names():
            print(f"Dropping collection: {coll_name}")
            db.drop_collection(coll_name)

if __name__ == '__main__':
    reset_category_collections()


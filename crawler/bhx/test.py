# #!/usr/bin/env python3
# # test_all_categories.py

# import asyncio
# from crawler.bhx.fetch_data import BHXDataFetcher, db
# from crawler.bhx.fetch_store_by_province import fetch_stores_async

# async def main():
#     # 1) Khởi tạo fetcher và intercept token
#     fetcher = BHXDataFetcher()
#     await fetcher.init_token()

#     # 2) Lấy danh sách stores ở TP. HCM (province_id = 3)
#     province_id = 3
#     district_id = 2087
#     ward_id = 27127
#     stores = await fetch_stores_async(
#         province_id=province_id,
#         token=fetcher.token,
#         deviceid=fetcher.deviceid,
#         district_id=district_id,
#         ward_id=ward_id,
#         page_size=100
#     )

#     if not stores:
#         print("❌ Không tìm thấy cửa hàng nào ở TP.HCM")
#         await fetcher.close()
#         return

#     # 3) Chọn cửa hàng đầu tiên để test
#     store = stores[0]
#     store_id   = store["storeId"]
#     ward_id    = store["wardId"]
#     district_id = store["districtId"]
#     print(f"🔎 Test crawl cửa hàng: {store.get('storeLocation')} (ID: {store_id})\n")

#     # 4) Đọc tất cả category từ MongoDB
#     categories = list(db.categories.find({}))
#     if not categories:
#         print("❌ Chưa có category nào trong DB.")
#         await fetcher.close()
#         return

#     total_products = 0

#     # 5) Loop qua từng category
#     for cat_doc in categories:
#         cat_name = cat_doc["name"]
#         links    = cat_doc.get("links", [])
#         if not links:
#             continue

#         print(f"=== Category: {cat_name} ({len(links)} link) ===")
#         for url in links:
#             print(f" → Crawling: https://www.bachhoaxanh.com/{url}")
#             products = await fetcher.fetch_product_info(
#                 province_id=province_id,
#                 ward_id=ward_id,
#                 district_id=district_id,
#                 store_id=store_id,
#                 category_url=url,
#                 isMobile=True,
#                 page_size=10
#             )
#             count = len(products)
#             total_products += count
#             print(f"   ✓ Lấy được {count} sản phẩm từ link này.\n")

#     print(f"🌟 Tổng cộng đã crawl {total_products} products cho cửa hàng {store_id}.\n")

#     # 6) Đóng fetcher
#     await fetcher.close()

# if __name__ == "__main__":
#     asyncio.run(main())

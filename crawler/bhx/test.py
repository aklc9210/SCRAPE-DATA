#!/usr/bin/env python3
import asyncio
import json
from crawler.bhx.demo import BHXDataFetcher  # chứa fetch_products_direct_api
from crawler.bhx.process_data import process_product_data

async def main():
    # 1) Init fetcher & token/deviceid
    fetcher = BHXDataFetcher()
    await fetcher.init_token()

    # 2) Chọn cửa hàng TP.HCM (province_id=3)
    province_id = 3
    # đây chỉ lấy ví dụ cửa hàng đầu tiên trong danh sách
    stores = await fetcher.fetch_all_stores_direct_api()
    if not stores:
        print("❌ Không có store nào.")
        await fetcher.close()
        return
    store = stores[0]
    store_id = store["storeId"]
    district_id = store.get("districtId", 0)
    ward_id     = store.get("wardId", 0)
    print(f"🔎 Test store #{store_id}: {store.get('storeLocation')}")

    # 3) Lấy danh sách categories từ DB
    from db import MongoDB
    db = MongoDB.get_db()
    categories = list(db.categories.find({}))
    if not categories:
        print("❌ Chưa có category nào trong DB.")
        await fetcher.close()
        return

    all_processed = []

    # 4) Với mỗi category → gọi API, then process
    for cat in categories:
        cat_name = cat["name"]
        links    = cat.get("links", [])
        if not links:
            continue
        print(f"\n=== Category: {cat_name} ({len(links)} links) ===")
        for url in links:
            print(f"→ Fetching raw products: {url}")
            raw_products = await fetcher.fetch_products_direct_api(
                store_id=store_id,
                category_url=url,
                province_id=province_id,
                district_id=district_id,
                ward_id=ward_id
            )
            print(f"   → Retrieved {len(raw_products)} raw items")
            
            for p in raw_products:
                try:
                    proc = process_product_data(
                        product=p,
                        category_name=cat_name,
                        store_id=store_id
                    )
                    if proc:
                        all_processed.append(proc)
                        print(f"   ✓ Processed SKU={proc['sku']} | Name: {proc['name_en']}")
                except Exception as ex:
                    print(f"   ⚠️ Skip product: {ex}")

    # 5) In tổng và lưu JSON
    print(f"\n🌟 Total processed products: {len(all_processed)}")
    out_file = f"store_{store_id}_processed.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(all_processed, f, ensure_ascii=False, indent=2)
    print(f"✅ Saved processed data to {out_file}")

    await fetcher.close()

if __name__ == "__main__":
    asyncio.run(main())

import requests
import pandas as pd
import time
# import ace_tools as tools

API_URL = "https://apibhx.tgdd.vn/Location/V2/GetStoresByLocation"
HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Authorization": "Bearer C141CABA8C779EE75D72B8BCA42C5DE6",
    "xapikey": "bhx-api-core-2022",
    "customer-id": "",
    "deviceid": "%7B%22did%22%3A%22783e4eae-0829-441b-b278-07f3dff488e7%22%7D",
    "platform": "webnew",
    "Referer": "https://www.bachhoaxanh.com/he-thong-cua-hang"
}

def fetch_stores(province_id: int, district_id: int = 0, ward_id: int = 0, page_size: int = 50):
    """
    Fetch all stores for the given province/district/ward by paginating until empty.
    """
    stores = []
    page_index = 0

    while True:
        params = {
            "provinceId": province_id,
            "districtId": district_id,
            "wardId": ward_id,
            "pageSize": page_size,
            "pageIndex": page_index
        }
        resp = requests.get(API_URL, headers=HEADERS, params=params)
        resp.raise_for_status()
        data = resp.json()
        batch = data.get("stores", [])
        if not batch:
            break
        stores.extend(batch)
        page_index += 1
        time.sleep(0.2)  # polite delay

    return stores

# def main():
#     # Example: fetch all stores in province 82 (Hồ Chí Minh)
#     stores_data = fetch_stores(province_id=82, page_size=100)

#     # Normalize into DataFrame
#     df = pd.json_normalize(stores_data)
#     df = df.rename(columns={
#         'storeId': 'store_id',
#         'lat': 'latitude',
#         'lng': 'longitude',
#         'storeLocation': 'store_location',
#         'provinceId': 'province_id',
#         'districtId': 'district_id',
#         'wardId': 'ward_id',
#         'isStoreVirtual': 'is_store_virtual',
#         'openHour': 'open_hour'
#     })

#     # Display DataFrame to user
#     tools.display_dataframe_to_user("Bách Hóa Xanh Stores (Province 82)", df)

#     # Optionally save to CSV
#     df.to_csv('/mnt/data/bhx_stores_province_82.csv', index=False)
#     print("Saved CSV to /mnt/data/bhx_stores_province_82.csv")

# if __name__ == "__main__":
#     main()

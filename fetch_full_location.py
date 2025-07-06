import requests
import pandas as pd
# import ace_tools as tools

# API endpoint to fetch full location data
FULL_API_URL = "https://apibhx.tgdd.vn/Location/V2/GetFull"
HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Authorization": "Bearer C141CABA8C779EE75D72B8BCA42C5DE6",
    "xapikey": "bhx-api-core-2022",
    "customer-id": "",
    "deviceid": "%7B%22did%22%3A%22783e4eae-0829-441b-b278-07f3dff488e7%22%7D",
    "platform": "webnew",
    "Referer": "https://www.bachhoaxanh.com/he-thong-cua-hang"
}

def fetch_full_location_data():
    """
    Fetches the full hierarchical location data:
    provinces, districts, wards, and storeInfos.
    """
    resp = requests.get(FULL_API_URL, headers=HEADERS)
    resp.raise_for_status()
    payload = resp.json().get("data", {})
    return payload

# def main():
#     data = fetch_full_location_data()

#     # Convert each list to a DataFrame
#     df_provinces = pd.DataFrame(data.get("provinces", []))
#     df_districts = pd.DataFrame(data.get("districts", []))
#     df_wards = pd.DataFrame(data.get("wards", []))
#     df_storeinfos = pd.DataFrame(data.get("storeInfos", []))

#     # Display DataFrames to the user
#     tools.display_dataframe_to_user("Provinces", df_provinces)
#     tools.display_dataframe_to_user("Districts", df_districts)
#     tools.display_dataframe_to_user("Wards", df_wards)
#     tools.display_dataframe_to_user("Store Infos", df_storeinfos)

#     # Optionally, save each to CSV
#     df_provinces.to_csv("/mnt/data/provinces_full.csv", index=False)
#     df_districts.to_csv("/mnt/data/districts_full.csv", index=False)
#     df_wards.to_csv("/mnt/data/wards_full.csv", index=False)
#     df_storeinfos.to_csv("/mnt/data/store_infos_full.csv", index=False)
#     print("CSV files saved to /mnt/data/")

# if __name__ == "__main__":
#     main()

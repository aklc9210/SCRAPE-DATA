import pandas as pd
import asyncio
from curl_cffi.requests import Session
from token_interceptor import BHXTokenInterceptor
from fetch_store_by_province import fetch_stores_async
from fetch_full_location import get_headers, FULL_API_URL

session = Session(impersonate="chrome110")

class BHXDataFetcher:
    def __init__(self):
        """Initialize fetcher - token will be intercepted automatically"""
        self.token = None
        self.deviceid = None
        self.interceptor = None
    
    async def init_token(self):
        """Initialize and get token automatically"""
        print("Initializing token interception...")
        self.interceptor = BHXTokenInterceptor()
        self.token, self.deviceid = await self.interceptor.init_and_get_token()
        
        if not self.token:
            raise Exception("Failed to intercept token. Please check your internet connection.")
        
        print(f"Token intercepted successfully!")
        return True
    
    async def fetch_all_stores(self):
        """Main method to fetch all stores data"""
        print('=== Starting BHX Stores Data Fetching ===')
        
        # 1) Initialize token
        await self.init_token()
        
        # 2) Get full location data
        print('Fetching provinces data...')
        headers = get_headers(self.token, self.deviceid)
        
        try:
            resp = session.get(FULL_API_URL, headers=headers, timeout=15)
            if resp.status_code != 200:
                raise Exception(f"Failed to fetch provinces: {resp.status_code}")
            
            loc_data = resp.json().get("data", {})
            provinces = loc_data.get("provinces", [])
            print(f"Found {len(provinces)} provinces.")
            
            # Export provinces.csv while we have the data
            self.export_provinces_csv(loc_data)
            
        except Exception as e:
            print(f"Error fetching provinces: {e}")
            return None

        if not provinces:
            print("No provinces found. Something went wrong.")
            return None

        all_records = []
        
        # 3) For each province, fetch its stores
        for i, prov in enumerate(provinces, 1):
            prov_id = prov.get("id")
            prov_name = prov.get("name", "")
            print(f"\n[{i}/{len(provinces)}] Fetching stores for {prov_name} (ID: {prov_id})...")
            
            try:
                stores = await fetch_stores_async(
                    province_id=prov_id,
                    token=self.token,
                    deviceid=self.deviceid,
                    district_id=0,
                    ward_id=0,
                    page_size=100
                )
                
                for s in stores:
                    s["province_id"] = prov_id
                    s["province_name"] = prov_name
                    
                all_records.extend(stores)
                print(f"✓ Found {len(stores)} stores in {prov_name}")
                
            except Exception as e:
                print(f"✗ Error fetching stores for {prov_name}: {e}")
                continue

        print(f"\n=== Fetching completed! Total stores: {len(all_records)} ===")
        return all_records
    
    def export_provinces_csv(self, loc_data):
        """Export provinces, districts, wards to CSV files"""
        try:
            # Export provinces
            provinces = loc_data.get("provinces", [])
            if provinces:
                df_provinces = pd.DataFrame(provinces)
                df_provinces.to_csv("provinces.csv", index=False, encoding="utf-8-sig")
                print(f"✓ Exported {len(provinces)} provinces to provinces.csv")
            
            # Export districts
            districts = loc_data.get("districts", [])
            if districts:
                df_districts = pd.DataFrame(districts)
                df_districts.to_csv("districts.csv", index=False, encoding="utf-8-sig")
                print(f"✓ Exported {len(districts)} districts to districts.csv")
            
            # Export wards
            wards = loc_data.get("wards", [])
            if wards:
                df_wards = pd.DataFrame(wards)
                df_wards.to_csv("wards.csv", index=False, encoding="utf-8-sig")
                print(f"✓ Exported {len(wards)} wards to wards.csv")
                
        except Exception as e:
            print(f"Error exporting location data: {e}")
    
    def save_to_csv(self, stores_data, filename="all_bhx_stores.csv"):
        """Save stores data to CSV with proper column mapping"""
        if not stores_data:
            print("No data to save.")
            return None
            
        print(f"Saving {len(stores_data)} stores to {filename}...")
        
        # Normalize to DataFrame
        df = pd.json_normalize(stores_data)
        
        # Rename columns
        column_mapping = {
            "storeId": "store_id",
            "lat": "latitude", 
            "lng": "longitude",
            "storeLocation": "store_location",
            "provinceId": "province_id_original",
            "districtId": "district_id",
            "wardId": "ward_id", 
            "isStoreVirtual": "is_store_virtual",
            "openHour": "open_hour",
            "phone": "phone_number",
            "status": "store_status"
        }
        
        df.rename(columns=column_mapping, inplace=True)
        
        # Save to CSV
        df.to_csv(filename, index=False, encoding="utf-8-sig")
        print(f"✓ Data saved to {filename}")
        
        return df
    
    async def close(self):
        """Clean up resources"""
        if self.interceptor:
            await self.interceptor.close()

async def main():
    """
    Main function - Automatically gets token and fetches all data
    """
    fetcher = BHXDataFetcher()
    
    try:
        # Fetch all stores
        stores_data = await fetcher.fetch_all_stores()
        
        if stores_data:
            # Save to CSV
            df = fetcher.save_to_csv(stores_data)
            
            # Print summary
            print(f"\n=== FINAL SUMMARY ===")
            print(f"Total stores: {len(stores_data)}")
            
            # Show stores count by province
            province_counts = {}
            for store in stores_data:
                prov = store.get('province_name', 'Unknown')
                province_counts[prov] = province_counts.get(prov, 0) + 1
            
            print(f"Provinces covered: {len(province_counts)}")
            print("\nTop 10 provinces by store count:")
            sorted_provinces = sorted(province_counts.items(), key=lambda x: x[1], reverse=True)
            for prov, count in sorted_provinces[:10]:
                print(f"  {prov}: {count} stores")
                
        else:
            print("Failed to fetch stores data.")
            
    except Exception as e:
        print(f"Error in main: {e}")
    finally:
        await fetcher.close()

def run_sync():
    """Synchronous entry point"""
    asyncio.run(main())

if __name__ == "__main__":
    run_sync()
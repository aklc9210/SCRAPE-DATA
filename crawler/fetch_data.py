import pandas as pd
import asyncio
from curl_cffi.requests import Session
from token_interceptor import BHXTokenInterceptor
from fetch_store_by_province import fetch_stores_async
from fetch_full_location import get_headers, FULL_API_URL
from fetch_menus_for_store import fetch_menu_for_store

session = Session(impersonate="chrome110")

# variables for category
valid_titles = set([
        # Thịt, cá, trứng
        "Thịt heo", "Thịt bò", "Thịt gà, vịt, chim", "Thịt sơ chế", "Trứng gà, vịt, cút",
        "Cá, hải sản, khô", "Cá hộp", "Lạp xưởng", "Xúc xích", "Heo, bò, pate hộp",
        "Chả giò, chả ram", "Chả lụa, thịt nguội", "Xúc xích, lạp xưởng tươi",
        "Cá viên, bò viên", "Thịt, cá đông lạnh",

        # Rau, củ, quả, nấm
        "Trái cây", "Rau lá", "Củ, quả", "Nấm các loại", "Rau, củ làm sẵn",
        "Rau củ đông lạnh",

        # Đồ ăn chay
        "Đồ chay ăn liền", "Đậu hũ, đồ chay khác", "Đậu hũ, tàu hũ",

        # Ngũ cốc, tinh bột
        "Ngũ cốc", "Ngũ cốc, yến mạch", "Gạo các loại", "Bột các loại",
        "Đậu, nấm, đồ khô",

        # Mì, bún, phở, cháo
        "Mì ăn liền", "Phở, bún ăn liền", "Hủ tiếu, miến", "Miến, hủ tiếu, phở khô",
        "Mì Ý, mì trứng", "Cháo gói, cháo tươi", "Bún các loại", "Nui các loại",
        "Bánh tráng các loại", "Bánh phồng, bánh đa", "Bánh gạo Hàn Quốc",

        # Gia vị, phụ gia, dầu
        "Nước mắm", "Nước tương", "Tương, chao các loại", "Tương ớt - đen, mayonnaise",
        "Dầu ăn", "Dầu hào, giấm, bơ", "Gia vị nêm sẵn", "Muối",
        "Hạt nêm, bột ngọt, bột canh", "Tiêu, sa tế, ớt bột", "Bột nghệ, tỏi, hồi, quế,...",
        "Nước chấm, mắm", "Mật ong, bột nghệ",

        # Sữa & các sản phẩm từ sữa
        "Sữa tươi", "Sữa đặc", "Sữa pha sẵn", "Sữa hạt, sữa đậu", "Sữa ca cao, lúa mạch",
        "Sữa trái cây, trà sữa", "Sữa chua ăn", "Sữa chua uống liền", "Bơ sữa, phô mai",

        # Đồ uống
        "Bia, nước có cồn", "Rượu", "Nước trà", "Nước ngọt", "Nước ép trái cây",
        "Nước yến", "Nước tăng lực, bù khoáng", "Nước suối", "Cà phê hoà tan",
        "Cà phê pha phin", "Cà phê lon", "Trà khô, túi lọc",

        # Bánh kẹo, snack
        "Bánh tươi, Sandwich", "Bánh bông lan", "Bánh quy", "Bánh snack, rong biển",
        "Bánh Chocopie", "Bánh gạo", "Bánh quế", "Bánh que", "Bánh xốp",
        "Kẹo cứng", "Kẹo dẻo, kẹo marshmallow", "Kẹo singum", "Socola",
        "Trái cây sấy", "Hạt khô", "Rong biển các loại", "Rau câu, thạch dừa",
        "Mứt trái cây", "Cơm cháy, bánh tráng",

        # Món ăn chế biến sẵn, đông lạnh
        "Làm sẵn, ăn liền", "Sơ chế, tẩm ướp", "Nước lẩu, viên thả lẩu",
        "Kim chi, đồ chua", "Mandu, há cảo, sủi cảo", "Bánh bao, bánh mì, pizza",
        "Kem cây, kem hộp", "Bánh flan, thạch, chè", "Trái cây hộp, siro",

        "Cá mắm, dưa mắm", "Đường", "Nước cốt dừa lon", "Sữa chua uống", "Khô chế biến sẵn"
    ])

categories_mapping = {
        # Thịt, cá, trứng
        "Thịt heo": "Fresh Meat",
        "Thịt bò": "Fresh Meat",
        "Thịt gà, vịt, chim": "Fresh Meat",
        "Thịt sơ chế": "Fresh Meat",
        "Trứng gà, vịt, cút": "Fresh Meat",
        "Cá, hải sản, khô": "Seafood & Fish Balls",
        "Cá hộp": "Instant Foods",
        "Lạp xưởng": "Cold Cuts: Sausages & Ham",
        "Xúc xích": "Cold Cuts: Sausages & Ham",
        "Heo, bò, pate hộp": "Instant Foods",
        "Chả giò, chả ram": "Instant Foods",
        "Chả lụa, thịt nguội": "Cold Cuts: Sausages & Ham",
        "Xúc xích, lạp xưởng tươi": "Cold Cuts: Sausages & Ham",
        "Cá viên, bò viên": "Instant Foods",
        "Thịt, cá đông lạnh": "Instant Foods",

        # Rau, củ, quả, nấm
        "Trái cây": "Fresh Fruits",
        "Rau lá": "Vegetables",
        "Củ, quả": "Vegetables",
        "Nấm các loại": "Vegetables",
        "Rau, củ làm sẵn": "Vegetables",
        "Rau củ đông lạnh": "Vegetables",

        # Đồ ăn chay
        "Đồ chay ăn liền": "Instant Foods",
        "Đậu hũ, đồ chay khác": "Instant Foods",
        "Đậu hũ, tàu hũ": "Instant Foods",

        # Ngũ cốc, tinh bột
        "Ngũ cốc": "Cereals & Grains",
        "Ngũ cốc, yến mạch": "Cereals & Grains",
        "Gạo các loại": "Grains & Staples",
        "Bột các loại": "Grains & Staples",
        "Đậu, nấm, đồ khô": "Grains & Staples",

        # Mì, bún, phở, cháo
        "Mì ăn liền": "Instant Foods",
        "Phở, bún ăn liền": "Instant Foods",
        "Hủ tiếu, miến": "Instant Foods",
        "Miến, hủ tiếu, phở khô": "Instant Foods",
        "Mì Ý, mì trứng": "Instant Foods",
        "Cháo gói, cháo tươi": "Instant Foods",
        "Bún các loại": "Instant Foods",
        "Nui các loại": "Instant Foods",
        "Bánh tráng các loại": "Instant Foods",
        "Bánh phồng, bánh đa": "Instant Foods",
        "Bánh gạo Hàn Quốc": "Cakes",

        # Gia vị, phụ gia, dầu
        "Nước mắm": "Seasonings",
        "Nước tương": "Seasonings",
        "Tương, chao các loại": "Seasonings",
        "Tương ớt - đen, mayonnaise": "Seasonings",
        "Dầu ăn": "Seasonings",
        "Dầu hào, giấm, bơ": "Seasonings",
        "Gia vị nêm sẵn": "Seasonings",
        "Muối": "Seasonings",
        "Hạt nêm, bột ngọt, bột canh": "Seasonings",
        "Tiêu, sa tế, ớt bột": "Seasonings",
        "Bột nghệ, tỏi, hồi, quế,...": "Seasonings",
        "Nước chấm, mắm": "Seasonings",
        "Mật ong, bột nghệ": "Seasonings",

        # Sữa & các sản phẩm từ sữa
        "Sữa tươi": "Milk",
        "Sữa đặc": "Milk",
        "Sữa pha sẵn": "Milk",
        "Sữa hạt, sữa đậu": "Milk",
        "Sữa ca cao, lúa mạch": "Milk",
        "Sữa trái cây, trà sữa": "Milk",
        "Sữa chua ăn": "Yogurt",
        "Sữa chua uống liền": "Yogurt",
        "Bơ sữa, phô mai": "Milk",

        # Đồ uống
        "Bia, nước có cồn": "Alcoholic Beverages",
        "Rượu": "Alcoholic Beverages",
        "Nước trà": "Beverages",
        "Nước ngọt": "Beverages",
        "Nước ép trái cây": "Beverages",
        "Nước yến": "Beverages",
        "Nước tăng lực, bù khoáng": "Beverages",
        "Nước suối": "Beverages",
        "Cà phê hoà tan": "Beverages",
        "Cà phê pha phin": "Beverages",
        "Cà phê lon": "Beverages",
        "Trà khô, túi lọc": "Beverages",

        # Bánh kẹo, snack
        "Bánh tươi, Sandwich": "Cakes",
        "Bánh bông lan": "Cakes",
        "Bánh quy": "Cakes",
        "Bánh snack, rong biển": "Snacks",
        "Bánh Chocopie": "Cakes",
        "Bánh gạo": "Cakes",
        "Bánh quế": "Cakes",
        "Bánh que": "Cakes",
        "Bánh xốp": "Cakes",
        "Kẹo cứng": "Candies",
        "Kẹo dẻo, kẹo marshmallow": "Candies",
        "Kẹo singum": "Candies",
        "Socola": "Candies",
        "Trái cây sấy": "Dried Fruits",
        "Hạt khô": "Dried Fruits",
        "Rong biển các loại": "Snacks",
        "Rau câu, thạch dừa": "Fruit Jam",
        "Mứt trái cây": "Fruit Jam",
        "Cơm cháy, bánh tráng": "Snacks",

        # Món ăn chế biến sẵn, đông lạnh
        "Làm sẵn, ăn liền": "Instant Foods",
        "Sơ chế, tẩm ướp": "Instant Foods",
        "Nước lẩu, viên thả lẩu": "Instant Foods",
        "Kim chi, đồ chua": "Instant Foods",
        "Mandu, há cảo, sủi cảo": "Instant Foods",
        "Bánh bao, bánh mì, pizza": "Instant Foods",
        "Kem cây, kem hộp": "Ice Cream & Cheese",
        "Bánh flan, thạch, chè": "Cakes",
        "Trái cây hộp, siro": "Fruit Jam",

        # Khác
        "Cá mắm, dưa mắm": "Seasonings",
        "Đường": "Seasonings",
        "Nước cốt dừa lon": "Seasonings",
        "Sữa chua uống": "Yogurt",
        "Khô chế biến sẵn": "Instant Foods"
    }

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
        catergories = []
        
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
                
            # fetch data menus with each store
            for s in stores:
                ward_id = s.get('wardId', 0)
                store_id = s.get('storeId')
                # attach basic info
                s['province_id'] = prov_id
                s['province_name'] = prov_name
                s['ward_id'] = ward_id
                s['store_id'] = store_id

                # fetch menu
                menus = await fetch_menu_for_store(prov_id, ward_id, store_id, self.token, self.deviceid)
                for menu in menus:
                    print(f"Danh mục cha: {menu['name']}")
                    
                    for child in menu.get("childrens", []):
                        category = categories_mapping.get(child['name'])

                        if child['name'] in valid_titles:
                            catergories.append({
                                "name": category,
                                "link": child['url']
                            })
                            print(f"Category: {category} - {child['name']}")
                        else:
                            print(f"{child['name']} (ID: {child['id']}) - bỏ qua")

                print(f"  • Menu fetched for store {store_id}")

            all_records.extend(stores)

        print(f"\n=== Fetching completed! Total stores: {len(all_records)} ===")
        return all_records, catergories
    
    def fetch_product_info(self, province_id, ward_id, district_id, store_id, isMobile=True, page_size=50):
        """
        Fetch product info for a specific store
        """
        headers = get_headers(self.token, self.deviceid, isMobile)
        url = f"https://apibhx.tgdd.vn/Category/V2/GetCate?provinceId={province_id}&wardId={ward_id}&districtId={district_id}&storeId={store_id}&categoryUrl={category_url}&isMobile={isMobile}&isV2=true&pageSize={page_size}"
        
        
        
        try:
            resp = session.get(url, headers=headers, timeout=15)
            if resp.status_code != 200:
                raise Exception(f"Failed to fetch products: {resp.status_code}")
            
            return resp.json().get("data", {}).get("products", [])
        
        except Exception as e:
            print(f"Error fetching products: {e}")
            return []
    
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
        stores_data, categories = await fetcher.fetch_all_stores()
        
        if stores_data:
            # Save to CSV
            df = fetcher.save_to_csv(stores_data)
            
            # Print summary
            print(f"\n=== STORE SUMMARY ===")
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

        if categories:
            df = fetcher.save_to_csv(categories, filename="categories.csv")
            print(f"\n=== CATEGORIES SUMMARY ===")
            print(f"Total categories: {len(categories)}")

            # Show top categories
            category_counts = {}
            for category in categories:
                cat_name = category.get('name', 'Unknown')
                category_counts[cat_name] = category_counts.get(cat_name, 0) + 1
            sorted_categories = sorted(category_counts.items(), key=lambda x: x[1], reverse=True)
            print("\nTop 10 categories by count:")
            for cat, count in sorted_categories[:10]:
                print(f"  {cat}: {count} items")
            
    except Exception as e:
        print(f"Error in main: {e}")
    finally:
        await fetcher.close()

def run_sync():
    """Synchronous entry point"""
    asyncio.run(main())

if __name__ == "__main__":
    run_sync()
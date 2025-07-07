from curl_cffi.requests import Session
import asyncio

session = Session(impersonate="chrome110")
MENU_API_URL = "https://apibhx.tgdd.vn/Menu/GetMenuV2"

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

def get_menu_headers(token, deviceid):
    """Generate headers for store fetching"""
    return {
        "Accept": "application/json, text/plain, */*",
        "Authorization": token,
        "xapikey": "bhx-api-core-2022",
        "platform": "webnew",
        "reversehost": "http://bhxapi.live",
        "origin": "https://www.bachhoaxanh.com",
        "referer": "https://www.bachhoaxanh.com/he-thong-cua-hang",
        "referer-url": "https://www.bachhoaxanh.com/he-thong-cua-hang",
        "content-type": "application/json",
        "deviceid": deviceid,
        "customer-id": "",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
    }

async def fetch_menu_for_store(province_id, ward_id, store_id, token: str, deviceid: str, int=50):
    """Fetch menu data for a single store using province_id, ward_id, store_id"""
    header = get_menu_headers(token, deviceid)
    menus = []
    page_index = 0

    while True:
        params = {
            "ProvinceId": province_id,
            "WardId": ward_id,
            "StoreId": store_id
        }

        try:
            resp = session.get(MENU_API_URL, headers=header, params=params)
            
            if resp.status_code != 200:
                print(f"Failed menu fetch for store {store_id}: {resp.status_code}")
                break

            data = resp.json()
            batch = data.get("data", {}).get("menus", [])
            total = data.get("data", {}).get("totalPromotions", 0)

            if not batch:
                print(f"No menu found for store {store_id}")
                break
            
            menus.extend(batch)
            print(f"Fetched {len(batch)} items for store {store_id} on page {page_index + 1}")
            page_index += 1
            
            if len(menus) >= total:
                print(f"All menu items fetched for store {store_id}")
                break

            await asyncio.sleep(0.3)
        except Exception as e:
            print(f"Error fetching menu for store {store_id}: {e}") 
            break

    return menus
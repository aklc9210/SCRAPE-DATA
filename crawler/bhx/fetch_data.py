import pandas as pd
import asyncio
import re
from typing import List
from curl_cffi.requests import Session
from crawler.bhx.token_interceptor import BHXTokenInterceptor, get_headers
from crawler.bhx.fetch_store_by_province import fetch_stores_async
from crawler.bhx.fetch_full_location import FULL_API_URL
from crawler.bhx.fetch_menus_for_store import fetch_menu_for_store
from db import MongoDB
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
from datetime import datetime
from pymongo import UpdateOne

session = Session(impersonate="chrome110")
db = MongoDB.get_db()

# Valid categories
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

# Standard categories
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

tokenizer_vi2en = AutoTokenizer.from_pretrained(
    "VietAI/envit5-translation",
    use_fast=False
)
model_vi2en = AutoModelForSeq2SeqLM.from_pretrained("VietAI/envit5-translation")

# transform to store_name and store_location
def parse_store_line(s: str):
    name = s.split('(', 1)[0].strip()

    start = s.find('(')
    if start == -1:
        return {"store_name": name, "store_location": ""}
    level = 0
    end = None
    for i, ch in enumerate(s[start:], start):
        if ch == '(':
            level += 1
        elif ch == ')':
            level -= 1
            if level == 0:
                end = i
                break
    content = s[start+1:end] if end else ""

    location = re.sub(r'\([^)]*\)', '', content).strip()
    location = location.strip(',').strip()

    return {
        "store_name": name,
        "store_location": location
    }

def translate_vi2en(vi_text: str) -> str:
    prompt = "translate Vietnamese to English: " + vi_text
    inputs = tokenizer_vi2en(prompt, return_tensors="pt")
    outputs = model_vi2en.generate(
        **inputs,
        num_beams=5,
        early_stopping=True,
        max_length=512
    )
    return tokenizer_vi2en.decode(outputs[0], skip_special_tokens=True)

def tokenize_by_whitespace(text: str) -> List[str]:
    if text is None:
        return []
    return [token for token in text.lower().split() if len(token) >= 2]

def generate_ngrams(token: str, n: int) -> List[str]:
    if token is None or len(token) < n:
        return []
    return [token[i:i+n] for i in range(len(token) - n + 1)]

def generate_token_ngrams(text: str, n: int) -> List[str]:
    tokens = tokenize_by_whitespace(text)
    ngrams = []
    for token in tokens:
        ngrams.extend(generate_ngrams(token, n))
    return ngrams

def extract_best_price(product: dict) -> dict:
    base_price_info = product.get("productPrices", [])
    campaign_info = product.get("lstCampaingInfo", [])
    name = product.get("name", "")
    original_unit = product.get("unit", "").lower()

    def build_result(info: dict, unit: str, net_value: float):
        return {
            "name": name,
            "unit": unit, 
            "netUnitValue": net_value,
            "price": info.get("price"),
            "sysPrice": info.get("sysPrice"),
            "discountPercent": info.get("discountPercent"),
            "date_begin": info.get("startTime") or info.get("poDate"),
            "date_end": info.get("dueTime") or info.get("poDate"),
        }
    
    # 1. Campaign ưu tiên
    if campaign_info:
        campaign = campaign_info[0]
        campaign_price = campaign.get("productPrice", {})
        net_value = campaign_price.get("netUnitValue", 0)

        net_value, converted_unit = normalize_net_value(original_unit, net_value, name)
        print(f"[Campaign] Product name: {name}, old unit: {original_unit}, netUnitValue: {net_value}")
        return build_result(campaign_price, converted_unit, net_value)

    # 2. Fallback sang base_price
    if base_price_info:
        price_info = base_price_info[0]
        net_value = price_info.get("netUnitValue", 0)

        net_value, converted_unit = normalize_net_value(original_unit, net_value, name)
        print(f"[BasePrice] Product name: {name}, old unit: {original_unit}, netUnitValue: {net_value}")
        return build_result(price_info, converted_unit, net_value)

    # 3. No info then u
    return {
        "name": name,
        "unit": original_unit,
        "netUnitValue": 1,
        "price": None,
        "sysPrice": None,
        "discountPercent": None,
        "date_begin": None,
        "date_end": None,
    }

def extract_net_value_and_unit_from_name(name: str, fallback_unit: str):
    tmp_name = name.lower()
    matches = re.findall(r"(\d+(?:\.\d+)?)\s*(g|ml|lít|kg|gói|l)\b", tmp_name)
    if matches:
        value, unit = matches[-1]  # use the LAST match
        return float(value), unit
    return 1, fallback_unit

def normalize_net_value(unit: str, net_value: float, name: str):
    unit = unit.lower()
    name_lower = name.lower()

    # 1. Nếu là đơn vị quy đổi thì nhân để tính lại netValue, NHƯNG GIỮ UNIT GỐC
    if unit == "kg":
        return float(net_value) * 1000, "g"
    elif unit == "lít":
        return float(net_value) * 1000, "ml"
    if unit not in ["kg", "g", "ml", "lít"]:
        match_kg = re.search(r"(\d+(\.\d+)?)\s*kg", name_lower)
        if match_kg:
            value = float(match_kg.group(1))
            return value * 1000, unit
    if unit == "túi 1kg":
        return float(net_value) * 1000, "túi"
    
    # 2. Túi có trái thì giả định 0.7kg
    if unit == "túi" and "trái" in name_lower:
        return 0.7 * 1000, unit

    # 3. Hộp hoặc vỉ có số lượng (quả trứng, ...)
    if unit in ["hộp", "vỉ"] and "quả" in name_lower:
        matches = re.findall(rf"{unit}\s*(\d+)", name_lower)
        if matches:
            return sum(map(int, matches)), unit

    # 4. Thùng / Lốc X đơn vị Y ml/g
    match_pack = re.search(r"(thùng|lốc)\s*(\d+).*?(\d+(\.\d+)?)\s*(g|ml)", name_lower)
    if match_pack:
        count = int(match_pack.group(2))
        per_item = float(match_pack.group(3))
        return count * per_item, unit

    # 5. Trường hợp fallback (gói, khay, ống...) — giữ nguyên unit gốc, chỉ đổi value nếu có thông tin
    extracted_value, _ = extract_net_value_and_unit_from_name(name, unit)
    if extracted_value > 0:
        return extracted_value, unit

    return float(net_value) if net_value != 0 else 1, unit

async def upsert_product(product_data, category_title):
    """Upsert product data into MongoDB collection named after its category."""
    # Chuyển tên category thành tên collection an toàn, ví dụ:
    coll_name = category_title.replace(" ", "_").lower()
    product_db = db[coll_name]

    sku = product_data.get("sku")
    if not sku:
        print("No SKU found for product, skipping upsert.")
        return

    # Upsert vào collection tương ứng
    product_db.update_one(
        {"sku": sku},
        {"$set": product_data},
        upsert=True
    )
    print(f"✓ Upserted product: {product_data.get('name', '')} "
          f"(SKU: {sku}) into collection: {coll_name}")

async def upsert_products_bulk(product_list: List[dict], category_title: str):
    """Bulk upsert danh sách product vào collection tương ứng."""
    coll_name = category_title.replace(" ", "_").lower()
    collection = db[coll_name]
    ops = []
    for p in product_list:
        sku = p.get("sku")
        if not sku:
            continue
        ops.append(
            UpdateOne(
                {"sku": sku},
                {"$set": p},
                upsert=True
            )
        )
    if not ops:
        return
    result = collection.bulk_write(ops, ordered=False)
    print(f"✓ Bulk upserted {len(ops)} products into `{coll_name}` "
          f"(upserted: {result.upserted_count}, modified: {result.modified_count})")


class BHXDataFetcher:
    def __init__(self):
        self.token = None
        self.deviceid = None
        self.interceptor = None
    
    async def init_token(self):
        print("Initializing token interception...")
        self.interceptor = BHXTokenInterceptor()
        self.token, self.deviceid = await self.interceptor.init_and_get_token()
        
        if not self.token:
            raise Exception("Failed to intercept token. Please check your internet connection.")
        
        print(f"Token intercepted successfully!")
        return True
    
    async def fetch_all_stores(self):
        print('=== Starting BHX Stores Data Fetching ===')
        
        # Initialize token
        await self.init_token() 
        
        # Get full location data
        print('Fetching provinces data...')
        headers = get_headers(self.token, self.deviceid)
        
        # fetch menu -> get categories
        categories = []
        menus = await fetch_menu_for_store(3, 2087, 4946, self.token, self.deviceid)
        for menu in menus:
            print(f"Danh mục cha: {menu['name']}")
            
            for child in menu.get("childrens", []):
                category = categories_mapping.get(child['name'])

                if child['name'] in valid_titles:
                    categories.append({
                        "name": category,
                        "link": child['url']
                    })
                    print(f"Category: {category} - {child['name']}")
                else:
                    print(f"{child['name']} (ID: {child['id']}) - bỏ qua")
        
        # Upsert categories to db
        if categories:
            category_db = db.category

            grouped = {}
            for cat in categories:
                name = cat.get('name')
                link = cat.get('link')
                if name and link:
                    grouped.setdefault(name, []).append(link)

            for name, links in grouped.items():
                unique_links = list(dict.fromkeys(links))
                category_db.update_one(
                    {"name": name},
                    {
                        "$set": {"links": unique_links},
                    },
                    upsert=True
                )
            print(f"✓ Upserted {len(grouped)} distinct categories to MongoDB.")

        # print(f"  • Menu fetched for store {store_id}")
        
        try:
            resp = session.get(FULL_API_URL, headers=headers, timeout=15)
            if resp.status_code != 200:
                raise Exception(f"Failed to fetch provinces: {resp.status_code}")
            
            loc_data = resp.json().get("data", {})
            provinces = loc_data.get("provinces", [])
            print(f"Found {len(provinces)} provinces.")
            
            # Upsert chain to db
            self.upsert_db(provinces)
            
        except Exception as e:
            print(f"Error fetching provinces: {e}")
            return None

        if not provinces:
            print("No provinces found. Something went wrong.")
            return None

        all_records = []
        

        # create a dict for provinces
        district_map = {}
        ward_map = {}
        for prov in provinces:
            for dist in prov.get("districts", []):
                district_map[dist["id"]] = dist["name"]
                for ward in dist.get("wards", []):
                    ward_map[ward["id"]] = ward["name"]
        
        # fetch each province stores
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
                
                all_products = []
                for s in stores:
                    ward_id     = s.get("wardId", 0)
                    district_id = s.get("districtId", 0)
                    store_id    = s.get('storeId')

                    # Gán cả ID lẫn tên
                    s["province_id"]    = prov_id
                    s["province"]       = prov_name
                    s["district_id"]    = district_id
                    s["district"]       = district_map.get(district_id, "")
                    s["ward_id"]        = ward_id
                    s["ward"]           = ward_map.get(ward_id, "")

                    # coll = db.category
                    # for cat_doc in coll.find({}):
                    #     for category_url in cat_doc.get("links", []):
                    #         products = await self.fetch_product_info(
                    #             province_id=prov_id,
                    #             ward_id=ward_id,
                    #             district_id=district_id,
                    #             store_id=store_id,
                    #             category_url=category_url
                    #         )
                            # print(f"Fetched {len(products)} products for category {category_url} in store {store_id}")
                    
                    coll = db.category
                    for cat_doc in coll.find({}):
                        for category_url in cat_doc.get("links", []):
                            prods = await self.fetch_product_info(
                                province_id=prov_id,
                                ward_id=ward_id,
                                district_id=district_id,
                                store_id=store_id,
                                category_url=category_url,
                                isMobile=True,
                                page_size=10
                            )
                            all_products.extend(prods)
                            total = len(prods)  
                        await upsert_products_bulk(all_products, cat_doc['name'])
                
                # Upsert store data
                all_records.extend(stores)
                print(f"✓ Found {len(stores)} stores in {prov_name}")
                
            except Exception as e:
                print(f"✗ Error fetching stores for {prov_name}: {e}")
                continue
            

        print(f"\n=== Fetching completed! Total stores: {len(all_records)} ===")
        return all_records
    
    # fetch product info -> later
    async def fetch_product_info(self, province_id, ward_id, district_id, store_id, category_url, isMobile=True, page_size=10):
        headers = get_headers(self.token, self.deviceid)

        headers.update({
            "referer": f"https://www.bachhoaxanh.com/{category_url}",
            "referer-url": f"https://www.bachhoaxanh.com/{category_url}",
            "origin": "https://www.bachhoaxanh.com",
        })

        url = f"https://apibhx.tgdd.vn/Category/V2/GetCate?provinceId={province_id}&wardId={ward_id}&districtId={district_id}&storeId={store_id}&categoryUrl={category_url}&isMobile={isMobile}&isV2=true&pageSize={page_size}"
          
        try:
            resp = session.get(url, headers=headers, timeout=15)
            if resp.status_code != 200:
                raise Exception(f"Failed to fetch products: {resp.status_code}")
            
            products = resp.json().get("data", {}).get("products", [])
            total = resp.json().get("data", {}).get("total", 0)

            cat = db.category.find_one({"links": category_url})
            
            product_records = []
            for product in products:
                english_name = translate_vi2en(product.get("name", ""))
                if not english_name:
                    print(f"Failed to translate name: {product.get('name', '')}")
                    continue
                price_info = extract_best_price(product)
                token_ngrams = generate_token_ngrams(english_name, 2)
                product_data = {
                    "sku": product["id"],
                    "name": product["name"],
                    "name_en": english_name,
                    "unit": price_info["unit"].lower(),
                    "netUnitValue": price_info["netUnitValue"], 
                    "token_ngrams": token_ngrams,
                    "category": cat["name"], 
                    "store_id": store_id, 
                    "url": f"https://www.bachhoaxanh.com{product['url']}",
                    "image": product["avatar"],
                    "promotion": product.get("promotionText", ""),
                    "price": price_info["price"],
                    "sysPrice": price_info["sysPrice"],
                    "discountPercent": price_info["discountPercent"],
                    "date_begin": price_info["date_begin"],
                    "date_end": price_info["date_end"],
                    "crawled_at": datetime.utcnow().isoformat(),
                }
            
                # print(f"Product data: {product_data}")
                product_records.append(product_data)
            print(f"Get successfully {total} products for {cat['name']}")

            return product_records
        
        except Exception as e:
            print(f"Error fetching products: {e}")
            return []
    
    # upsert chain to mongodb
    def upsert_db(self, loc_data):
        try:
            chain_db = db.chain
            provinces = loc_data
            if provinces:
                for prov in provinces:
                    prov_id = prov.get("id")
                    prov_name = prov.get("name", "")
                    prov_district = prov.get("districts", [])
                    
                    chain_db.update_one(
                        {"_id": prov_id},
                        {"$set": {"name": prov_name, "district": prov_district}},
                        upsert=True
                    )
                    print(f"✓ Upserted province: {prov_name} (ID: {prov_id})")
            
        except Exception as e:
            print(f"Error upserting data: {e}")

    
    # upsert store to mongodb
    def upsert_store_db(self, stores_data):
        if not stores_data:
            print("No data to save.")
            return None
            
        print(f"Saving {len(stores_data)} stores to db...")

        store_db = db.store
        for store in stores_data:
            store_id = store.get("storeId")
            if not store_id:
                continue

            store_title = parse_store_line(store.get("storeLocation", ""))
            
            # Prepare data for upsert
            store_data = {
                "store_id": store_id,
                "store_name": store_title['store_name'],
                "latitude": store.get("lat", 0.0),
                "longitude": store.get("lng", 0.0),
                "store_location": store_title['store_location'],
                "province_id": store.get("provinceId", 0),
                "province": store.get("province", ""),
                "district_id": store.get("districtId", 0),
                "district": store.get("district", ""),
                "ward_id": store.get("wardId", 0),
                "ward": store.get("ward", ""),
                "is_store_virtual": store.get("isStoreVirtual", False),
                "open_hour": store.get("openHour", ""),
                "phone_number": store.get("phone", ""),
                "store_status": store.get("status", "")
            }
            
            # Upsert to MongoDB
            store_db.update_one(
                {"store_id": store_id},
                {"$set": store_data},
                upsert=True
            )

        print(f"✓ Upserted {len(stores_data)} stores to MongoDB.")
    
    async def close(self):
        """Clean up resources"""
        if self.interceptor:
            await self.interceptor.close()

async def main():
    fetcher = BHXDataFetcher()
    
    try:
        stores_data = await fetcher.fetch_all_stores()
        
        if stores_data:
            df = fetcher.upsert_store_db(stores_data)
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
            
    except Exception as e:
        print(f"Error in main: {e}")
    finally:
        await fetcher.close()

def run_sync():
    """Synchronous entry point"""
    asyncio.run(main())

if __name__ == "__main__":
    run_sync()
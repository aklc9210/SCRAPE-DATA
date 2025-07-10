import requests
from typing import List, Dict

class WinMartCategoryFetcher: 
    def __init__(self):
        self.api_base = "https://api-crownx.winmart.vn/mt/api/web/v1"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'vi-VN,vi;q=0.9,en;q=0.8'
        }
        
        # Category mapping to standardized names
        self.category_mapping = {
            # Milk & Dairy products
            "Sữa các loại": "Milk",
            "Sữa Tươi": "Milk",
            "Sữa Hạt - Sữa Đậu": "Milk",
            "Sữa Bột": "Milk",
            "Bơ Sữa - Phô Mai": "Milk",
            "Sữa đặc": "Milk",
            "Sữa Chua - Váng Sữa": "Yogurt",
            "Sữa Bột - Sữa Dinh Dưỡng": "Milk",
            
            # Vegetables & Fruits
            "Rau - Củ - Trái Cây": "Vegetables",
            "Rau Lá": "Vegetables",
            "Củ, Quả": "Vegetables",
            "Trái cây tươi": "Fresh Fruits",
            
            # Meat, Seafood, Eggs
            "Thịt - Hải Sản Tươi": "Fresh Meat",
            "Thịt": "Fresh Meat",
            "Hải Sản": "Seafood & Fish Balls",
            "Trứng - Đậu Hũ": "Fresh Meat",
            "Trứng": "Fresh Meat",
            "Đậu hũ": "Instant Foods",
            "Thịt Đông Lạnh": "Instant Foods",
            "Hải Sản Đông Lạnh": "Instant Foods",
            
            # Bakery & Confectionery
            "Bánh Kẹo": "Cakes",
            "Bánh Xốp - Bánh Quy": "Cakes",
            "Kẹo - Chocolate": "Candies",
            "Bánh Snack": "Snacks",
            "Hạt - Trái Cây Sấy Khô": "Dried Fruits",
            
            # Beverages
            "Đồ uống có cồn": "Alcoholic Beverages",
            "Bia": "Alcoholic Beverages",
            "Đồ Uống - Giải Khát": "Beverages",
            "Cà Phê": "Beverages",
            "Nước Suối": "Beverages",
            "Nước Ngọt": "Beverages",
            "Trà - Các Loại Khác": "Beverages",
            
            # Instant Foods
            "Mì - Thực Phẩm Ăn Liền": "Instant Foods",
            "Mì": "Instant Foods",
            "Miến - Hủ Tíu - Bánh Canh": "Instant Foods",
            "Cháo": "Instant Foods",
            "Phở - Bún": "Instant Foods",
            
            # Dry Foods & Grains
            "Thực Phẩm Khô": "Grains & Staples",
            "Gạo - Nông Sản Khô": "Grains & Staples",
            "Ngũ Cốc - Yến Mạch": "Cereals & Grains",
            "Thực Phẩm Đóng Hộp": "Instant Foods",
            "Rong Biển - Tảo Biển": "Snacks",
            "Bột Các Loại": "Grains & Staples",
            "Thực Phẩm Chay": "Instant Foods",
            
            # Processed Foods
            "Thực Phẩm Chế Biến": "Instant Foods",
            "Bánh mì": "Instant Foods",
            "Xúc xích - Thịt Nguội": "Cold Cuts: Sausages & Ham",
            "Bánh bao": "Instant Foods",
            "Kim chi": "Instant Foods",
            "Thực Phẩm Chế Biến Khác": "Instant Foods",
            
            # Seasonings
            "Gia vị": "Seasonings",
            "Dầu Ăn": "Seasonings",
            "Nước Mắm - Nước Chấm": "Seasonings",
            "Đường": "Seasonings",
            "Nước Tương": "Seasonings",
            "Hạt Nêm": "Seasonings",
            "Tương Các Loại": "Seasonings",
            "Gia Vị Khác": "Seasonings",
            
            # Frozen Foods
            "Thực Phẩm Đông Lạnh": "Instant Foods",
            "Chả Giò": "Instant Foods",
            "Cá - Bò Viên": "Instant Foods",
            "Thực Phẩm Đông Lạnh Khác": "Instant Foods"
        }
    
    def fetch_categories(self) -> List[Dict]:
        url = f"{self.api_base}/category"
        
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            
            if response.status_code != 200:
                raise Exception(f"API returned status {response.status_code}")
            
            data = response.json()
            
            if data.get('code') != 'S200':
                raise Exception(f"API error: {data.get('message', 'Unknown error')}")
            
            categories = self._extract_categories(data.get('data', []))
            print(f"Found {len(categories)} food categories")
            
            return categories
            
        except requests.RequestException as e:
            raise Exception(f"Request failed: {e}")
        except Exception as e:
            raise Exception(f"Failed to fetch categories: {e}")
    
    def _extract_categories(self, data: List[Dict]) -> List[Dict]:
        categories = []
        
        for category_item in data:
            parent = category_item.get('parent', {})
            children = category_item.get('lstChild', [])
            
            # Process main category
            parent_name = parent.get('name', '')
            if parent.get('seoName') and self._is_food_category(parent_name):
                categories.append({
                    'name': parent_name,
                    'code': parent.get('code', ''),
                    'slug': parent.get('seoName', ''),
                    'level': parent.get('level', 1),
                    'parent_name': parent_name,
                    'mapped_category': self.category_mapping.get(parent_name, parent_name)
                })
            
            # Process subcategories
            for child in children:
                child_parent = child.get('parent', {})
                child_name = child_parent.get('name', '')
                
                if child_parent.get('seoName') and self._is_food_category(child_name):
                    categories.append({
                        'name': child_name,
                        'code': child_parent.get('code', ''),
                        'slug': child_parent.get('seoName', ''),
                        'level': child_parent.get('level', 2),
                        'parent_name': parent_name,
                        'mapped_category': self.category_mapping.get(child_name, child_name)
                    })
        
        return categories
    
    def _is_food_category(self, category_name: str) -> bool:
        return category_name in self.category_mapping
    

def get_food_categories() -> List[Dict]:
    fetcher = WinMartCategoryFetcher()
    return fetcher.fetch_categories()

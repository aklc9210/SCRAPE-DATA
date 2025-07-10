import requests
from typing import List, Dict, Optional

class WinMartBranchFetcher:    
    def __init__(self):
        self.api_base = "https://api-crownx.winmart.vn/mt/api/web/v1"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'vi-VN,vi;q=0.9,en;q=0.8'
        }
        # Focus on Ho Chi Minh City only
        self.province_codes = ['HCM']
    
    def fetch_branches(self) -> List[Dict]:
        all_stores = []
        
        for province_code in self.province_codes:
            try:
                stores = self._fetch_stores_by_province(province_code)
                all_stores.extend(stores)
                print(f" {province_code}: {len(stores)} stores found")
            except Exception as e:
                print(f" {province_code}: {e}")
        
        return all_stores
    
    def _fetch_stores_by_province(self, province_code: str) -> List[Dict]:
        url = f"{self.api_base}/store-by-province"
        params = {'PageNumber': 1, 'PageSize': 1000, 'ProvinceCode': province_code}
        
        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=10)
            
            if response.status_code != 200:
                raise Exception(f"API returned status {response.status_code}")
            
            data = response.json()
            return self._parse_api_response(data)
            
        except requests.RequestException as e:
            raise Exception(f"Request failed: {e}")
        except Exception as e:
            raise Exception(f"Failed to parse response: {e}")
    
    def _parse_api_response(self, data: Dict) -> List[Dict]:
        stores = []
        
        province_name = data.get('provinceName', '')
        districts_data = data.get('data', [])
        
        for district in districts_data:
            ward_stores = district.get('wardStores', [])
            
            for ward in ward_stores:
                ward_stores_list = ward.get('stores', [])
                
                for store in ward_stores_list:
                    normalized_store = self._normalize_store_data(store, province_name)
                    if normalized_store:
                        stores.append(normalized_store)
        
        return stores
    
    def _normalize_store_data(self, store: Dict, province_name: str) -> Optional[Dict]:
        store_code = store.get('storeCode')
        store_name = store.get('storeName')
        
        if not store_code or not store_name:
            return None
        
        # Determine chain type based on chainId
        chain_id = store.get('chainId', '')
        if chain_id == 'VMT':
            chain = 'winmart'  # WinMart (large supermarkets)
        elif chain_id == 'VMP':
            chain = 'winmart+'  # WinMart+ (convenience stores)
        else:
            chain = 'winmart'
        
        return {
            'store_id': store_code,
            'chain': chain,
            'name': store_name.strip(),
            'address': store.get('officeAddress', '').strip(),
            'phone': store.get('contactMobile', '').strip() or store.get('officeNumber', '').strip(),
            'province': province_name,
            'district': store.get('districtName', ''),
            'ward': store.get('wardName', ''),
            'province_code': store.get('provinceCode', ''),
            'district_code': store.get('districtCode', ''),
            'ward_code': store.get('wardCode', ''),
            'lat': float(store.get('latitude', 0)) if store.get('latitude') else None,
            'lng': float(store.get('longitude', 0)) if store.get('longitude') else None,
            'is_open': store.get('isOpen', False),
            'support_delivery': store.get('supportDelivering', False),
            'delivery_status': store.get('deliveryStatus', ''),
            'active_status': store.get('activeStatus', ''),
            'allow_cod': store.get('allowCod', False)
        }

def get_active_stores() -> List[Dict]:
    fetcher = WinMartBranchFetcher()
    all_stores = fetcher.fetch_branches()
    
    active_stores = [
        store for store in all_stores 
        if store.get('active_status', '').strip() == ''
    ]
    
    return active_stores

if __name__ == "__main__":
    fetcher = WinMartBranchFetcher()
    branches = fetcher.fetch_branches()
    print(f"Total branches found: {len(branches)}")
    
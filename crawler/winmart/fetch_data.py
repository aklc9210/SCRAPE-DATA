import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[2]))

from typing import List, Dict
from pathlib import Path

from fetch_branches import WinMartBranchFetcher
from fetch_category import WinMartCategoryFetcher

from db import MongoDB

class WinMartDataCoordinator:
    
    def __init__(self, output_dir: str = "output"):
        self.db = MongoDB.get_db()
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        self.branch_fetcher = WinMartBranchFetcher()
        self.category_fetcher = WinMartCategoryFetcher()
    
    def fetch_and_save_to_db(self) -> Dict[str, any]:
        saved_stats = {}
        
        branches = self.branch_fetcher.fetch_branches()
        active_branches = [b for b in branches if b.get('active_status', '').strip() == '']
        
        branches_saved = self._save_branches_to_db(active_branches)
        saved_stats['branches'] = branches_saved
        
        return saved_stats
    
    def _save_branches_to_db(self, branches: List[Dict]) -> int:
        if not branches:
            return 0
        
        collection = self.db.stores
        saved_count = 0
        
        fieldnames = [
            'store_id', 'chain', 'name', 'address', 'phone',
            'province', 'district', 'ward', 'lat', 'lng'
        ]
        
        for branch in branches:
            filtered_branch = {field: branch.get(field, '') for field in fieldnames}
            
            filter_query = {
                'store_id': branch.get('store_id'),
                'chain': branch.get('chain')
            }
            
            result = collection.update_one(
                filter_query,
                {'$set': filtered_branch},
                upsert=True
            )
            
            if result.upserted_id or result.modified_count > 0:
                saved_count += 1
        
        print(f"Saved {saved_count} branches to MongoDB")
        return saved_count

def main():
    coordinator = WinMartDataCoordinator()
    result = coordinator.fetch_and_save_to_db()
     
    return result

if __name__ == "__main__":
    main()
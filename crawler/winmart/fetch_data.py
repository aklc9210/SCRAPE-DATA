import csv
from datetime import datetime
from typing import List, Dict
from pathlib import Path

from fetch_branches import WinMartBranchFetcher
from fetch_category import WinMartCategoryFetcher

class WinMartDataCoordinator:
    """Coordinates WinMart data collection for stores and categories only"""
    
    def __init__(self, output_dir: str = "output"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        self.branch_fetcher = WinMartBranchFetcher()
        self.category_fetcher = WinMartCategoryFetcher()
    
    def fetch_and_save_basic_data(self) -> Dict[str, str]:
        """Fetch stores and categories, save to CSV"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        saved_files = {}
        
        # 1. Fetch and save branches
        branches = self.branch_fetcher.fetch_branches()
        active_branches = [b for b in branches if b.get('active_status', '').strip() == '']
        
        branches_file = self.output_dir / f"winmart_branches_{timestamp}.csv"
        self._save_branches_csv(active_branches, branches_file)
        saved_files['branches'] = str(branches_file)
        
        # 2. Fetch and save categories
        categories = self.category_fetcher.fetch_categories()
        
        categories_file = self.output_dir / f"winmart_categories_{timestamp}.csv"
        self._save_categories_csv(categories, categories_file)
        saved_files['categories'] = str(categories_file)
        
        saved_files['stats'] = {
            'branches': len(active_branches),
            'categories': len(categories)
        }
        
        return saved_files
    
    def _save_branches_csv(self, branches: List[Dict], filename: str):
        """Save branches to CSV"""
        fieldnames = [
            'store_id', 'chain', 'name', 'address', 'phone',
            'province', 'district', 'ward', 'lat', 'lng'
        ]
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for branch in branches:
                row = {field: branch.get(field, '') for field in fieldnames}
                writer.writerow(row)
    
    def _save_categories_csv(self, categories: List[Dict], filename: str):
        """Save categories to CSV"""
        fieldnames = ['name', 'mapped_category']
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for category in categories:
                row = {field: category.get(field, '') for field in fieldnames}
                writer.writerow(row)

def main():
    """Main execution function"""
    coordinator = WinMartDataCoordinator()
    result = coordinator.fetch_and_save_basic_data()
    
    # Show results
    if 'stats' in result:
        stats = result['stats']
        print(f"Saved: {stats['branches']} branches, {stats['categories']} categories")
        print(f"Files: {result['branches']}, {result['categories']}")
    
    return result

if __name__ == "__main__":
    main()
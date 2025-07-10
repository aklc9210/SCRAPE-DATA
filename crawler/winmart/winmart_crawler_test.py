import asyncio
import json
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from typing import List, Dict, Optional
import time

class WinMartCrawlerTest:
    def __init__(self):
        self.base_url = "https://winmart.vn"
        self.browser = None
        self.context = None
        self.page = None
        
    async def init_browser(self):
        """Initialize browser with Playwright"""
        try:
            p = await async_playwright().start()
            self.browser = await p.chromium.launch(
                headless=False,  # Set to True for production
                args=['--no-sandbox', '--disable-dev-shm-usage']
            )
            self.context = await self.browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            self.page = await self.context.new_page()
            
            # Listen for network requests to capture API calls
            self.captured_requests = []
            self.page.on("request", self._capture_request)
            self.page.on("response", self._capture_response)
            
            print("✅ Browser initialized successfully")
            return True
        except Exception as e:
            print(f"❌ Failed to initialize browser: {e}")
            return False
    
    def _capture_request(self, request):
        """Capture outgoing requests to find API endpoints"""
        if 'api' in request.url.lower() or 'ajax' in request.url.lower():
            self.captured_requests.append({
                'url': request.url,
                'method': request.method,
                'headers': dict(request.headers),
                'post_data': request.post_data
            })
    
    def _capture_response(self, response):
        """Capture API responses"""
        if 'api' in response.url.lower() or 'ajax' in response.url.lower():
            print(f"📡 API Response: {response.status} - {response.url}")

    async def close_browser(self):
        """Close browser"""
        if self.browser:
            await self.browser.close()
            print("🔐 Browser closed")

    async def test_basic_access(self):
        """Test basic website access"""
        print("\n🔍 Testing basic website access...")
        try:
            await self.page.goto(self.base_url, timeout=30000)
            await self.page.wait_for_load_state("networkidle")
            
            title = await self.page.title()
            print(f"✅ Page title: {title}")
            
            # Check if page loaded properly
            content = await self.page.content()
            if len(content) > 1000:
                print(f"✅ Page content loaded: {len(content)} characters")
                return True
            else:
                print(f"⚠️ Minimal content loaded: {len(content)} characters")
                return False
                
        except Exception as e:
            print(f"❌ Failed to access website: {e}")
            return False

    async def analyze_page_structure(self):
        """Analyze the page structure and find key elements"""
        print("\n🏗️ Analyzing page structure...")
        try:
            # Wait for page to fully load
            await self.page.wait_for_timeout(3000)
            
            # Try to find common e-commerce elements
            selectors_to_check = [
                'nav', 'header', 'footer', 
                '[class*="menu"]', '[class*="category"]', '[class*="product"]',
                '[class*="store"]', '[class*="branch"]', '[class*="location"]',
                'script[type="application/ld+json"]',  # Structured data
                '[data-*]',  # Data attributes that might indicate dynamic content
            ]
            
            found_elements = {}
            for selector in selectors_to_check:
                elements = await self.page.query_selector_all(selector)
                if elements:
                    found_elements[selector] = len(elements)
                    print(f"  ✅ Found {len(elements)} elements with selector: {selector}")
            
            # Check for Vue.js or React indicators
            vue_check = await self.page.evaluate("() => window.Vue !== undefined")
            react_check = await self.page.evaluate("() => window.React !== undefined")
            
            if vue_check:
                print("  🔵 Vue.js detected")
            if react_check:
                print("  🔵 React detected")
                
            return found_elements
            
        except Exception as e:
            print(f"❌ Failed to analyze page structure: {e}")
            return {}

    async def test_navigation_links(self):
        """Test navigation to different sections"""
        print("\n🧭 Testing navigation links...")
        
        test_urls = [
            f"{self.base_url}/categories",
            f"{self.base_url}/info/danh-sach-cua-hang",
            f"{self.base_url}/products",
            f"{self.base_url}/stores"
        ]
        
        results = {}
        for url in test_urls:
            try:
                print(f"  Testing: {url}")
                response = await self.page.goto(url, timeout=15000)
                await self.page.wait_for_load_state("networkidle")
                
                title = await self.page.title()
                content_length = len(await self.page.content())
                
                results[url] = {
                    'status': response.status,
                    'title': title,
                    'content_length': content_length,
                    'accessible': response.status == 200 and content_length > 1000
                }
                
                if results[url]['accessible']:
                    print(f"    ✅ Accessible - {title}")
                else:
                    print(f"    ❌ Not accessible or minimal content")
                    
            except Exception as e:
                print(f"    ❌ Error accessing {url}: {e}")
                results[url] = {'error': str(e)}
                
        return results

    async def search_for_api_endpoints(self):
        """Search for API endpoints by browsing the site"""
        print("\n🔍 Searching for API endpoints...")
        
        # Clear previous captures
        self.captured_requests = []
        
        try:
            # Navigate and interact with the site to trigger API calls
            await self.page.goto(self.base_url)
            await self.page.wait_for_load_state("networkidle")
            
            # Try to click on category links if they exist
            category_links = await self.page.query_selector_all('a[href*="categor"]')
            if category_links and len(category_links) > 0:
                print(f"  📁 Found {len(category_links)} category links, testing first one...")
                await category_links[0].click()
                await self.page.wait_for_load_state("networkidle")
                await self.page.wait_for_timeout(2000)
            
            # Try to find and click store/location links
            store_links = await self.page.query_selector_all('a[href*="store"], a[href*="cua-hang"]')
            if store_links and len(store_links) > 0:
                print(f"  🏪 Found {len(store_links)} store links, testing first one...")
                await store_links[0].click()
                await self.page.wait_for_load_state("networkidle")
                await self.page.wait_for_timeout(2000)
            
            # Check captured API calls
            if self.captured_requests:
                print(f"  📡 Captured {len(self.captured_requests)} API requests:")
                for i, req in enumerate(self.captured_requests[:5]):  # Show first 5
                    print(f"    {i+1}. {req['method']} {req['url']}")
            else:
                print("  ⚠️ No API requests captured")
                
            return self.captured_requests
            
        except Exception as e:
            print(f"❌ Error searching for API endpoints: {e}")
            return []

    async def test_requests_library(self):
        """Test direct HTTP requests without browser"""
        print("\n🌐 Testing direct HTTP requests...")
        
        test_urls = [
            self.base_url,
            f"{self.base_url}/categories",
            f"{self.base_url}/api/products",  # Common API path
            f"{self.base_url}/api/stores",    # Common API path
        ]
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'vi-VN,vi;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        
        for url in test_urls:
            try:
                print(f"  Testing: {url}")
                response = requests.get(url, headers=headers, timeout=10)
                content_length = len(response.content)
                
                print(f"    Status: {response.status_code}")
                print(f"    Content-Length: {content_length}")
                print(f"    Content-Type: {response.headers.get('content-type', 'Unknown')}")
                
                if content_length > 1000:
                    print(f"    ✅ Good content size")
                else:
                    print(f"    ⚠️ Minimal content")
                    
            except Exception as e:
                print(f"    ❌ Error: {e}")

    async def extract_page_data(self):
        """Try to extract any available data from the current page"""
        print("\n📊 Extracting available data...")
        
        try:
            await self.page.goto(self.base_url)
            await self.page.wait_for_load_state("networkidle")
            await self.page.wait_for_timeout(3000)
            
            # Get page content and parse with BeautifulSoup
            content = await self.page.content()
            soup = BeautifulSoup(content, 'html.parser')
            
            # Look for structured data
            json_scripts = soup.find_all('script', {'type': 'application/ld+json'})
            if json_scripts:
                print(f"  📋 Found {len(json_scripts)} structured data scripts")
                for i, script in enumerate(json_scripts[:2]):  # Show first 2
                    try:
                        data = json.loads(script.string)
                        print(f"    Script {i+1}: {type(data)} - {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
                    except:
                        print(f"    Script {i+1}: Invalid JSON")
            
            # Look for product/store data in text
            links = soup.find_all('a', href=True)
            categories = [link for link in links if 'category' in link.get('href', '').lower()]
            stores = [link for link in links if any(word in link.get('href', '').lower() for word in ['store', 'cua-hang'])]
            
            print(f"  🔗 Found {len(links)} total links")
            print(f"  📁 Found {len(categories)} category links")
            print(f"  🏪 Found {len(stores)} store links")
            
            # Try to extract any visible text that might be data
            text_content = soup.get_text()
            if 'WinMart' in text_content:
                print(f"  ✅ WinMart branding found in content")
            
            return {
                'total_links': len(links),
                'category_links': len(categories),
                'store_links': len(stores),
                'structured_data': len(json_scripts),
                'content_length': len(content)
            }
            
        except Exception as e:
            print(f"❌ Error extracting data: {e}")
            return {}

    async def run_full_test(self):
        """Run all tests"""
        print("🚀 Starting WinMart Crawler Test Suite")
        print("=" * 50)
        
        results = {}
        
        # Initialize browser
        if not await self.init_browser():
            return {"error": "Failed to initialize browser"}
        
        try:
            # Run all tests
            results['basic_access'] = await self.test_basic_access()
            results['page_structure'] = await self.analyze_page_structure()
            results['navigation'] = await self.test_navigation_links()
            results['api_search'] = await self.search_for_api_endpoints()
            results['data_extraction'] = await self.extract_page_data()
            
            # Test without browser
            await self.test_requests_library()
            
        finally:
            await self.close_browser()
        
        # Summary
        print("\n" + "=" * 50)
        print("📋 TEST SUMMARY")
        print("=" * 50)
        
        if results.get('basic_access'):
            print("✅ Basic website access: SUCCESS")
        else:
            print("❌ Basic website access: FAILED")
            
        if results.get('api_search'):
            print(f"📡 API endpoints found: {len(results['api_search'])}")
        else:
            print("⚠️ No API endpoints detected")
            
        if results.get('navigation'):
            accessible_pages = sum(1 for page in results['navigation'].values() if page.get('accessible'))
            print(f"🧭 Accessible pages: {accessible_pages}/{len(results['navigation'])}")
        
        print("\n💡 RECOMMENDATIONS:")
        if results.get('api_search'):
            print("   - API endpoints found - focus on reverse engineering these")
        else:
            print("   - No APIs found - will need full browser automation like CoopOnline")
            
        if results.get('basic_access'):
            print("   - Website is accessible - proceed with browser-based crawling")
        else:
            print("   - Website access issues - check for anti-bot measures")
            
        return results


# Helper function to run the test
async def main():
    crawler = WinMartCrawlerTest()
    results = await crawler.run_full_test()
    return results

if __name__ == "__main__":
    # Run the test
    results = asyncio.run(main())
    
    # Save results to file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    with open(f"winmart_test_results_{timestamp}.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"\n💾 Results saved to winmart_test_results_{timestamp}.json")
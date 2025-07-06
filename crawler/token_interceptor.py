import asyncio
import json
import uuid
from playwright.async_api import async_playwright
from urllib.parse import unquote

class BHXTokenInterceptor:
    def __init__(self):
        self.browser = None
        self.context = None
        self.page = None
        self.playwright = None 
        self.token = None
        self.deviceid = None

    async def init_and_get_token(self):
        """Initialize browser and intercept token automatically"""
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=True)
        self.context = await self.browser.new_context()
        self.page = await self.context.new_page()
        
        def log_request(request):
            # Intercept token from API requests
            if "Menu/GetMenuV2" in request.url or "Location/V2/GetStoresByLocation" in request.url or "Location/V2/GetFull" in request.url:
                print("Intercepted request to:", request.url)
                for k, v in request.headers.items():
                    if k.lower() == "authorization":
                        self.token = v
                        print(f"Found Bearer token: {v}")

        self.page.on("request", log_request)
        
        try:
            print("Loading BHX page to intercept token...")
            await self.page.goto("https://www.bachhoaxanh.com/he-thong-cua-hang", 
                                wait_until="domcontentloaded", timeout=20000)
            await self.page.wait_for_timeout(5000)
            
            # Get deviceid from cookie
            cookies = await self.context.cookies()
            ck_bhx_cookie = next((c["value"] for c in cookies if c["name"] == "ck_bhx_us_log"), None)

            if ck_bhx_cookie:
                try:
                    decoded = unquote(ck_bhx_cookie)
                    self.deviceid = json.loads(decoded).get("did")
                    print(f"Found deviceid: {self.deviceid}")
                except Exception as e:
                    print("Failed to parse deviceid from cookie:", e)
                    self.deviceid = str(uuid.uuid4())
            else:
                self.deviceid = str(uuid.uuid4())
                
            if not self.token:
                print("Token not intercepted yet, triggering more requests...")
                # Try to trigger more API calls
                await self.page.reload()
                await self.page.wait_for_timeout(3000)
                
        except Exception as e:
            print("Failed to load page:", e)

        return self.token, self.deviceid

    async def close(self):
        """Close browser"""
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
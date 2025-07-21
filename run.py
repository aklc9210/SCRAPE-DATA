# from crawler.winmart.demo import main
# from crawler.bhx.demo import run_sync
import asyncio  
from crawler.winmart.demo import WinMartFetcher

if __name__ == "__main__":
    asyncio.run(WinMartFetcher().run())
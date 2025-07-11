# from crawler.bhx.fetch_data import run_sync

from crawler.winmart.only_product import main
import asyncio

if __name__ == "__main__":
    asyncio.run(main())
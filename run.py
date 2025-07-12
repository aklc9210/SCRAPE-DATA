# from crawler.bhx.fetch_data import run_sync

from crawler.winmart.only_product import main
from crawler.bhx.fetch_data import run_sync
import asyncio

if __name__ == "__main__":
    main(run_sync())
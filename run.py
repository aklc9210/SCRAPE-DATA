from crawler.bhx.demo import run_sync

from crawler.winmart.only_product import main
import asyncio

if __name__ == "__main__":
    # asyncio.run(main())
    main(run_sync())
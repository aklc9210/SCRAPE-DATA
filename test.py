import asyncio
from db.db_async import get_db

async def main():
    db = get_db()  # bỏ await
    collections = await db.list_collection_names()  # vẫn await vì là async
    print(collections)

if __name__ == "__main__":
    asyncio.run(main())
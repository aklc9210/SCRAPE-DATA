from motor.motor_asyncio import AsyncIOMotorClient
import os
import asyncio

async def init_db():
    uri = os.getenv("MONGO_URI")
    if not uri:
        raise RuntimeError("Thiếu MONGO_URI trong biến môi trường")
    client = AsyncIOMotorClient(uri)
    try:
        await client.admin.command("ping")
        print("Kết nối MongoDB thành công.")
    except Exception as e:
        raise RuntimeError(f"Kết nối MongoDB thất bại: {e}")
    return client.store_recommender

db = asyncio.get_event_loop().run_until_complete(init_db())
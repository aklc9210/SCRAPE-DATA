from motor.motor_asyncio import AsyncIOMotorClient
import os

_client = AsyncIOMotorClient(os.getenv("MONGO_URI"))
db = _client.store_recommender
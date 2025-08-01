# db_async.py
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
import os

load_dotenv(override=True)


uri = os.getenv("MONGO_URI")

_client = None
def get_db():
    global _client
    if _client is None:
        _client = AsyncIOMotorClient(uri)
        print(f"Connected to MongoDB successfully. URI: {uri}")
    return _client.markendation


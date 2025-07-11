from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

uri = os.getenv("MONGO_URI")
if not uri:
    raise RuntimeError("Missing MONGO_URI in environment")

class MongoDB:
    _client = None
    _db = None

    @classmethod
    def get_db(cls):
        if cls._client is None:
            cls._client = MongoClient(uri)
            cls._db = cls._client.store_recommender
            print("Connected to MongoDB successfully.")
        return cls._db

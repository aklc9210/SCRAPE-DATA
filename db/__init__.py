from pymongo import MongoClient

class MongoDB:
    _client = None
    _db = None

    @classmethod
    def get_db(cls):
        if cls._client is None:
            cls._client = MongoClient("mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000")
            cls._db = cls._client.store_recommender
            print("Connected to MongoDB successfully.")
        return cls._db

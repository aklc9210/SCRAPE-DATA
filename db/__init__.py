from pymongo import MongoClient

uri = "mongodb+srv://n21dccn078:ABC12345678@storerecommender.w9auorn.mongodb.net/store_recommender?retryWrites=true&w=majority"

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

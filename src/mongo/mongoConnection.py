from pymongo import MongoClient, errors

class MongoConnection:

    def __init__(self):
        self.mongo_conn = None
        self.create_mongo_connection()

    def create_mongo_connection(self):
        try:
            self.mongo_conn = MongoClient(host='localhost', port=27017,
                                 serverSelectionTimeoutMS=3000)
        except errors.ServerSelectionTimeoutError as err:
            print("pymongo ERROR:", err)


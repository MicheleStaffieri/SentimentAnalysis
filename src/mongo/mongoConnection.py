from pprint import pprint

from pymongo import MongoClient, errors


class MongoConnection:

    def __init__(self):
        self.mongo_conn = None
        self.create_mongo_connection()

    def create_mongo_connection(self):
        try:
            self.mongo_conn = MongoClient(host='mongodb://127.0.0.1:27117,127.0.0.1:27118').maadb_project_db
        except errors.ServerSelectionTimeoutError as err:
            print("pymongo ERROR:", err)

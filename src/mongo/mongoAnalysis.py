import time
from pprint import pprint

from pymongo import MongoClient
from wordcloud import WordCloud

class MongoAnalysis:

    def __init__(self, mongo_conn):
        self.conn = mongo_conn
        self.calculate_hashtags_statistics()
        self.calculate_emojis_statistics()
        self.calculate_emoticons_statistics()

    def calculate_hashtags_statistics(self):
        db = self.conn
        collection = db.Twitter6
        start = time.time()
        pipeline = [
            {
                '$group': {
                    '_id': '$sentiment',
                    'hashtags': {
                        '$push': {
                            '$filter': {
                                'input': '$content.hashtags',
                                'as': 'hashtag',
                                'cond': {'$ne': ['$$hashtag', None]}
                            }
                        }
                    }
                }
            },
            {
                '$project': {
                    '_id': '$_id',
                    'hashtags': {
                        '$reduce': {
                            'input': '$hashtags',
                            'initialValue': [],
                            'in': {'$concatArrays': ['$$value', '$$this']}
                        }
                    }
                }
            },
            {
                '$project': {
                    '_id': '$_id',
                    'hashtags': {
                        '$reduce': {
                            'input': '$hashtags',
                            'initialValue': [],
                            'in': {'$concatArrays': ['$$value', '$$this']}
                        }
                    }
                }
            },
            {
                '$unwind': '$hashtags'
            },
            { # count the number of occurrences of each hashtag
                '$group': {
                    '_id': '$hashtags',
                    'total_occurrences': {
                        '$sum': 1
                    },
                    'sentiment': {
                        '$first': '$_id'
                    }
                }

            },
            {
                '$sort': {
                    'total_occurrences': -1
                }
            },
            {
                '$group': {
                    '_id': '$sentiment',
                    'hashtags': {
                        '$push': {
                            'hashtag': '$_id',
                            'total_occurrences': '$total_occurrences'
                        }
                    }
                }
            }
        ]
        cursor = collection.aggregate(pipeline)
        end = time.time()
        print(f"Hashtags calculated in: {end - start}")


    def calculate_emojis_statistics(self):
        db = self.conn
        collection = db.Twitter6
        start = time.time()
        pipeline = [
            {
                '$group': {
                    '_id': '$sentiment',
                    'emojis': {
                        '$push': {
                            '$filter': {
                                'input': '$content.emojis',
                                'as': 'emojis',
                                'cond': {'$ne': ['$$emojis', None]}
                            }
                        }
                    }
                }
            },
            {
                '$project': {
                    '_id': '$_id',
                    'emojis': {
                        '$reduce': {
                            'input': '$emojis',
                            'initialValue': [],
                            'in': {'$concatArrays': ['$$value', '$$this']}
                        }
                    }
                }
            },
            {
                '$project': {
                    '_id': '$_id',
                    'emojis': {
                        '$reduce': {
                            'input': '$emojis',
                            'initialValue': [],
                            'in': {'$concatArrays': ['$$value', '$$this']}
                        }
                    }
                }
            },
            {
                '$unwind': '$emojis'
            },
            {  # count the number of occurrences of each hashtag
                '$group': {
                    '_id': '$emojis',
                    'total_occurrences': {
                        '$sum': 1
                    },
                    'sentiment': {
                        '$first': '$_id'
                    }
                }

            },
            {
                '$sort': {
                    'total_occurrences': -1
                }
            },
            {
                '$group': {
                    '_id': '$sentiment',
                    'emojis': {
                        '$push': {
                            'emojis': '$_id',
                            'total_occurrences': '$total_occurrences'
                        }
                    }
                }
            }
        ]
        cursor = collection.aggregate(pipeline)
        end = time.time()
        print(f"Emojis calculated in: {end - start}")

    def calculate_emoticons_statistics(self):
        db = self.conn
        collection = db.Twitter6
        start = time.time()
        pipeline = [
            {
                '$group': {
                    '_id': '$sentiment',
                    'emoticons': {
                        '$push': {
                            '$filter': {
                                'input': '$content.emoticons',
                                'as': 'emoticons',
                                'cond': {'$ne': ['$$emoticons', None]}
                            }
                        }
                    }
                }
            },
            {
                '$project': {
                    '_id': '$_id',
                    'emoticons': {
                        '$reduce': {
                            'input': '$emoticons',
                            'initialValue': [],
                            'in': {'$concatArrays': ['$$value', '$$this']}
                        }
                    }
                }
            },
            {
                '$project': {
                    '_id': '$_id',
                    'emoticons': {
                        '$reduce': {
                            'input': '$emoticons',
                            'initialValue': [],
                            'in': {'$concatArrays': ['$$value', '$$this']}
                        }
                    }
                }
            },
            {
                '$unwind': '$emoticons'
            },
            {  # count the number of occurrences of each hashtag
                '$group': {
                    '_id': '$emoticons',
                    'total_occurrences': {
                        '$sum': 1
                    },
                    'sentiment': {
                        '$first': '$_id'
                    }
                }

            },
            {
                '$sort': {
                    'total_occurrences': -1
                }
            },
            {
                '$group': {
                    '_id': '$sentiment',
                    'emoticons': {
                        '$push': {
                            'emoticons': '$_id',
                            'total_occurrences': '$total_occurrences'
                        }
                    }
                }
            }
        ]
        cursor = collection.aggregate(pipeline)
        end = time.time()
        print(f"Emoticons calculated in: {end - start}")


if __name__ == '__main__':
    mongo_conn = MongoClient(host='mongodb://127.0.0.1:27117,127.0.0.1:27118').progetto
    MongoAnalysis(mongo_conn)

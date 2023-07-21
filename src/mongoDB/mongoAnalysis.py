import time
from pprint import pprint

from pymongo import MongoClient
from wordcloud import WordCloud

from src.utils.feeling_list import feeling_list


class MongoAnalysis:

    def __init__(self, mongo_conn):
        self.conn = mongo_conn
        self.words = {}
        self.hashtags = {}
        self.emojis = {}
        self.emoticons = {}
        self.calculate_hashtags_emojis_emoticons_statistics()
        self.calculate_words_statistics()
        self.calculate_words_in_res()

    def calculate_hashtags_emojis_emoticons_statistics(self):
        db = self.conn
        collection = db.Twitter6

        params = ['hashtags', 'emojis', 'emoticons']
        for param in params:
            start = time.time()
            pipeline = [
                # group by sentiment and push all hashtags in an array
                {
                    '$group': {
                        '_id': '$sentiment',
                        f'{param}': {
                            '$push': {
                                '$filter': {
                                    'input': f'$content.{param}',
                                    'as': f'{param}',
                                    'cond': {'$ne': [f'$${param}', None]}
                                }
                            }
                        }
                    }
                },
                # flatten the array of arrays
                {
                    '$project': {
                        '_id': '$_id',
                        f'{param}': {
                            '$reduce': {
                                'input': f'${param}',
                                'initialValue': [],
                                'in': {'$concatArrays': ['$$value', '$$this']}
                            }
                        }
                    }
                },
                {
                    '$project': {
                        '_id': '$_id',
                        f'{param}': {
                            '$reduce': {
                                'input': f'${param}',
                                'initialValue': [],
                                'in': {'$concatArrays': ['$$value', '$$this']}
                            }
                        }
                    }
                },
                # unwind the array of hashtags
                {
                    '$unwind': f'${param}'
                },
                {
                    '$group': {
                        '_id': {
                            'sentiment': '$_id',
                            f'{param}': f'${param}'
                        },
                        'total_occurrences': {
                            '$sum': 1
                        }
                    }
                },
                {
                    '$group': {
                        '_id': '$_id.sentiment',
                        f'{param}': {
                            '$push': {
                                f'{param}': f'$_id.{param}',
                                'total_occurrences': '$total_occurrences'
                            }
                        }
                    }
                },
            ]
            cursor = collection.aggregate(pipeline)
            for doc in cursor:
                pprint(doc)
            end = time.time()
            print(f"{param} calculated in: {end - start}")

    def calculate_words_statistics(self):
        db = self.conn
        collection = db.Twitter6
        for feeling in feeling_list:
            start = time.time()
            pipeline = [
                {
                    '$match': {
                        'sentiment': f'{feeling}'
                    }
                },
                {
                    '$project': {
                        'content': '$content'
                    }
                },
                {
                    '$project': {
                        '_id': '$_id',
                        'words': {
                            '$reduce': {
                                'input': '$content.words',
                                'initialValue': [],
                                'in': {'$concatArrays': ['$$value', '$$this']}
                            }
                        }
                    }
                },
                {
                    '$unwind': '$words'
                },
                {
                    '$group': {
                        '_id': '$words.lemma',
                        'total_occurrences': {
                            '$sum': 1
                        }
                    }
                },
                {
                    '$project': {
                        'word_count': {
                            'word': '$_id',
                            'count': '$total_occurrences'
                        }
                    }
                }
            ]
            cursor = collection.aggregate(pipeline)
            for doc in cursor:
                pprint(doc['word_count'])
            end = time.time()
            print(f"{feeling} calculated in: {end - start}")

    def calculate_words_in_res(self):
        db = self.conn
        collection = db.Twitter6
        for feeling in feeling_list:
            start = time.time()
            pipeline = [
                {
                    '$match': {
                        'sentiment': f'{feeling}'
                    }
                },
                {
                    '$project': {
                        'content': '$content'
                    }
                },
                {
                    '$project': {
                        '_id': '$_id',
                        'words': {
                            '$reduce': {
                                'input': '$content.words',
                                'initialValue': [],
                                'in': {'$concatArrays': ['$$value', '$$this']}
                            }
                        }
                    }
                },
                {
                    '$unwind': '$words'
                },
                {
                    '$group': {
                        '_id': {
                            'lemma': '$words.lemma',
                            'res': '$words.in_lex_resources'
                        },
                    }
                },
                {
                    '$group': {
                        '_id': None,
                        'total_documents_grouped': {'$sum': 1},
                        'grouped_data': {'$addToSet': '$_id'}
                    }
                },
                {
                    '$lookup': {
                        'from': 'LexResourcesWords2',
                        'localField': 'grouped_data.res.$id',
                        'foreignField': '_id',
                        'as': 'lex_res'
                    }
                },
                {
                    '$project': {
                        '_id': 0,
                        'total_documents_grouped': 1,
                        'lex_res': '$lex_res'
                    }
                },
                {
                    '$unwind': '$lex_res'
                },
                {
                    '$lookup': {
                        'from': 'LexResources3',
                        'localField': 'lex_res.resources.$id',
                        'foreignField': '_id',
                        'as': 'final_lex_res'
                    }
                },
                {
                    '$project': {
                        'total_documents_grouped': 1,
                        'lemma': '$lex_res.lemma',
                        'final_lex_res': {
                            '$filter': {
                                'input': '$final_lex_res',
                                'as': 'res',
                                'cond': {'$eq': ['$$res.sentiment', f'{feeling}']}
                            }
                        }
                    }
                },
                {
                    '$unwind': '$final_lex_res'
                },
                {
                    '$group': {
                        '_id': '$final_lex_res._id',
                        'res': {'$first': '$final_lex_res.totNumberWords'},
                        'total_documents_grouped': {'$first': '$total_documents_grouped'},
                        'total_occurrences': {
                            '$sum': 1
                        }
                    }
                },
                {
                    '$project': {
                        'perc_presence_lex_rex': {
                            '$multiply': [{'$divide': ['$total_occurrences', '$res']}, 100]
                        },
                        'perc_presence_twitter': {
                            '$multiply': [{'$divide': ['$total_occurrences', '$total_documents_grouped']}, 100]
                        }
                    }
                }
            ]
            # 0.015125668145115432
            cursor = collection.aggregate(pipeline)
            for doc in cursor:
                pprint(doc)
            end = time.time()
            print(f"{feeling} calculated in: {end - start}")


if __name__ == '__main__':
    mongo_conn = MongoClient(host='mongodb://127.0.0.1:27117,127.0.0.1:27118').progetto
    MongoAnalysis(mongo_conn)

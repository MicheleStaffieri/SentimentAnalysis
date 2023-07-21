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
        # self.calculate_hashtags_emojis_emoticons_statistics()
        # self.calculate_words_frequency()
        self.calculate_words_in_res()
        # self.create_word_clouds()

    def calculate_hashtags_emojis_emoticons_statistics(self):
        db = self.conn
        collection = db.Twitter6

        for feeling in feeling_list:
            self.hashtags[feeling] = {}
            self.emojis[feeling] = {}
            self.emoticons[feeling] = {}

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
                elem_list = doc[f'{param}']
                for elem in elem_list:
                    if param == 'hashtags':
                        self.hashtags[doc['_id']][elem[f'{param}']] = elem['total_occurrences']
                    elif param == 'emojis':
                        self.emojis[doc['_id']][elem[f'{param}']] = elem['total_occurrences']
                    elif param == 'emoticons':
                        self.emoticons[doc['_id']][elem[f'{param}']] = elem['total_occurrences']
            end = time.time()
            print(f"{param} calculated in: {end - start}")

    def calculate_words_frequency(self):
        db = self.conn
        collection = db.Twitter6
        for feeling in feeling_list:
            self.words[feeling] = {}
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
                },
            ]
            cursor = collection.aggregate(pipeline)
            for doc in cursor:
                word = doc['word_count']['word']
                count = doc['word_count']['count']
                self.words[feeling][word] = count
            pprint(self.words)
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
                        'from': 'grouped_data.res.$ref',
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
                        'from': 'lex_res.resources.$ref',
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
            cursor = collection.aggregate(pipeline)
            for doc in cursor:
                pprint(doc)
                result = open(f"../newResources/MongoStats/{feeling}_{doc['_id']}.txt", "w")
                result.write(
                    f"Percentuale presenza delle parole delle risorse lessicali nei tweets: {doc['perc_presence_lex_rex']}\n")
                result.write(
                    f"Percentuale presenza delle parole dei tweets nelle risorse lessicali : {doc['perc_presence_twitter']}\n")
            end = time.time()
            print(f"{feeling} calculated in: {end - start}")

    def create_word_clouds(self):
        for feeling in feeling_list:
            wordcloud_words = WordCloud(max_font_size=50, background_color="white", width=800,
                                        height=400).generate_from_frequencies(self.words[feeling])
            wordcloud_words.to_file(f"../newResources/WordClouds_mongo/{feeling}/cloud_words_" + feeling + ".png")
            wordcloud_hashtags = WordCloud(max_font_size=50, background_color="white", width=800,
                                           height=400).generate_from_frequencies(self.hashtags[feeling])
            wordcloud_hashtags.to_file(f"../newResources/WordClouds_mongo/{feeling}/cloud_hashtags_" + feeling + ".png")
            wordcloud_emoticons = WordCloud(max_font_size=50, background_color="white", width=800,
                                            height=400).generate_from_frequencies(self.emoticons[feeling])
            wordcloud_emoticons.to_file(
                f"../newResources/WordClouds_mongo/{feeling}/cloud_emoticons_" + feeling + ".png")
            wordcloud_emojis = WordCloud(max_font_size=50, background_color="white", width=800,
                                         height=400).generate_from_frequencies(self.emojis[feeling])
            wordcloud_emojis.to_file(f"../newResources/WordClouds_mongo/{feeling}/cloud_emojis_" + feeling + ".png")


if __name__ == '__main__':
    mongo_conn = MongoClient(host='mongodb://127.0.0.1:27117,127.0.0.1:27118').progetto
    MongoAnalysis(mongo_conn)

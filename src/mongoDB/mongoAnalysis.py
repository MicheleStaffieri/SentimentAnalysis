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
        self.calculate_words_frequency()
        self.calculate_top_10()
        self.create_word_clouds()
        self.calculate_words_in_res()



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
            pprint(f"Starting {param} analysis")
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
                # flatten the list of lists
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
                # flatten the list of lists again
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
                # group by sentiment and {param} and count the occurrences
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
                # group by sentiment and push all {param} in an array with the total occurrences
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
        pprint("Starting words frequency analysis")
        for feeling in feeling_list:
            self.words[feeling] = {}
            start = time.time()
            pipeline = [
                # filter by sentiment
                {
                    '$match': {
                        'sentiment': f'{feeling}'
                    }
                },
                # project only the content
                {
                    '$project': {
                        'content': '$content'
                    }
                },
                # flatten the list of lists
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
                # unwind the array of words
                {
                    '$unwind': '$words'
                },
                #  group by lemma and count the occurrences
                {
                    '$group': {
                        '_id': '$words.lemma',
                        'total_occurrences': {
                            '$sum': 1
                        }
                    }
                },
                # project only the lemma and the total occurrences
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
                pprint(doc['word_count'])
                word = doc['word_count']['word']
                count = doc['word_count']['count']
                self.words[feeling][word] = count
            end = time.time()
            print(f"{feeling} calculated in: {end - start}")

    def calculate_words_in_res(self):
        db = self.conn
        collection = db.Twitter6
        pprint("Starting words in tweets and resources analysis")
        for feeling in feeling_list:
            start = time.time()
            pipeline = [
                # filter by sentiment
                {
                    '$match': {
                        'sentiment': f'{feeling}'
                    }
                },
                # project only the content
                {
                    '$project': {
                        'content': '$content'
                    }
                },
                # flatten the list of lists
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
                # unwind the array of words
                {
                    '$unwind': '$words'
                },
                # group by lemma and in_lex_resource
                {
                    '$group': {
                        '_id': {
                            'lemma': '$words.lemma',
                            'res': '$words.in_lex_resources'
                        },
                    }
                },
                # count the occurrences of the differents lemma
                {
                    '$group': {
                        '_id': None,
                        'total_documents_grouped': {'$sum': 1},
                        'grouped_data': {'$addToSet': '$_id'}
                    }
                },
                # lookup the LexResourcesWords2 collection
                {
                    '$lookup': {
                        'from': 'LexResourcesWords2',
                        'localField': 'grouped_data.res.$id',
                        'foreignField': '_id',
                        'as': 'lex_res'
                    }
                },
                # project only the lex_res field and the total_documents_grouped
                {
                    '$project': {
                        '_id': 0,
                        'total_documents_grouped': 1,
                        'lex_res': '$lex_res'
                    }
                },
                # unwind the array of lex_res
                {
                    '$unwind': '$lex_res'
                },
                # lookup the LexResources3 collection
                {
                    '$lookup': {
                        'from': 'LexResources3',
                        'localField': 'lex_res.resources.$id',
                        'foreignField': '_id',
                        'as': 'final_lex_res'
                    }
                },
                # project total_documents_grouped, the lemma and the final_lex_res, a list of lex_res with the sentiment
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
                # unwind the array of final_lex_res
                {
                    '$unwind': '$final_lex_res'
                },
                # group by the id of the final_lex_res
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
                        '_id': 1,
                        'res': 1,
                        'total_documents_grouped': 1,
                        'total_occurrences': 1,
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
                result = open(f"../newResources/MongoStats/{feeling}_{doc['_id']}.txt", "w")
                result.write(f"Dati\n")
                result.write(f"Risorsa lessicale: {doc['_id']}\n")
                result.write(f"Numero totale parole risorsa lessicale: {doc['res']}\n")
                result.write(f"Numero totale tweets: {doc['total_documents_grouped']}\n")
                result.write(f"Numero di parole nell'intersezione: {doc['total_occurrences']}\n")

                result.write(
                    f"Percentuale presenza delle parole delle risorse lessicali nei tweets: {doc['perc_presence_lex_rex']}\n")
                result.write(
                    f"Percentuale presenza delle parole dei tweets nelle risorse lessicali : {doc['perc_presence_twitter']}\n")
            end = time.time()
            print(f"Statistics for {feeling} calculated in: {end - start}")

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

    def calculate_top_10(self):
        for feeling in feeling_list:

            sorted_hashtags = sorted(self.hashtags[feeling].items(), key=lambda x: x[1], reverse=True)[:10]
            result = open(f"../newResources/MongoTop10/{feeling}/hashtags_top10.txt", "w")
            for hashtag in sorted_hashtags:
                result.write(f"{hashtag[0]}: {hashtag[1]}\n")

            sorted_emoticons = sorted(self.emoticons[feeling].items(), key=lambda x: x[1], reverse=True)[:10]
            result = open(f"../newResources/MongoTop10/{feeling}/emoticons_top10.txt", "w")
            for emoticon in sorted_emoticons:
                result.write(f"{emoticon[0]}: {emoticon[1]}\n")

            sorted_emojis = sorted(self.emojis[feeling].items(), key=lambda x: x[1], reverse=True)[:10]
            result = open(f"../newResources/MongoTop10/{feeling}/emojis_top10.txt", "w")
            for emoji in sorted_emojis:
                result.write(f"{emoji[0]}: {emoji[1]}\n")

            sorted_words = sorted(self.words[feeling].items(), key=lambda x: x[1], reverse=True)[:10]
            result = open(f"../newResources/MongoTop10/{feeling}/words_top10.txt", "w")
            for word in sorted_words:
                result.write(f"{word[0]}: {word[1]}\n")


if __name__ == '__main__':
    mongo_conn = MongoClient(host='mongodb://127.0.0.1:27117,127.0.0.1:27118').progetto
    MongoAnalysis(mongo_conn)

from pprint import pprint

from bson import ObjectId

from src.utils.feeling_list import feeling_list

enum_feeling = {
    'Anger': 1,
    'Anticipation': 2,
    'Disgust': 3,
    'Fear': 4,
    'Joy': 5,
    'Sadness': 6,
    'Surprise': 7,
    'Trust': 8
}


class MongoPopulation:

    def __init__(self, mongo_conn, lex_resources, lex_resources_words, resources, tweets, emoji, hashtag, word_pos):
        self.lex_resources = lex_resources
        self.lex_resources_words = lex_resources_words
        self.resources = resources
        self.tweets = tweets
        self.emoji = emoji
        self.hashtag = hashtag
        self.conn = mongo_conn
        self.word_pos = word_pos
        # self.create_resources_table()
        self.create_twitter_table()

    def create_resources_table(self):
        db = self.conn
        collection = db.LexResources
        collection.drop()
        for res, values in self.lex_resources.items():
            collection.insert_one({
                '_id': res,
                'sentiment': values['sentiment'],
                'totNumberWords': values['totNumberWords']
            })
        collection = db.LexResourcesWords
        collection.drop()
        for word, resources in self.lex_resources_words.items():
            insertion_id = collection.insert_one({
                'lemma': word
            }).inserted_id
            for res in resources:
                collection.update_one({'_id': insertion_id},
                                      {'$push':
                                           {'resources':
                                                {'$ref': 'LexResources', '$id': res}
                                            }
                                       })

    def create_twitter_table(self):
        db = self.conn
        collection = db.Twitter
        collection.drop()

        for feeling, words in self.tweets.items():
            insertion_id = collection.insert_one({
                'sentiment': feeling,
                'doc_number': enum_feeling[feeling],
                'emoji': self.emoji[feeling],
                'hashtag': self.hashtag[feeling],
            }).inserted_id

            for word, freq in words.items():
                collection.update_one({'_id': insertion_id},
                                      {'$push':
                                           {'words': {'lemma': word,
                                                      'pos': self.word_pos[word],
                                                      'freq': freq,
                                                      'in_lex_resources':
                                                          {'$ref': 'LexResourcesWords',
                                                           '$id': word
                                                           },
                                                      }
                                            }
                                       })

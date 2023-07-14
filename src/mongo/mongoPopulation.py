import time
from pprint import pprint

from bson import ObjectId
from pymongo import UpdateOne

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
        self.create_resources_table()
        self.create_twitter_table()

    def create_resources_table(self):
        db = self.conn
        collection = db.LexResources
        collection.drop()
        # start time here
        start = time.time()
        lex_resources_bulk = []
        for res, values in self.lex_resources.items():
            lex_resources_bulk.append({
                '_id': res,
                'sentiment': values['sentiment'],
                'totNumberWords': values['totNumberWords']
            })

        collection.insert_many(lex_resources_bulk)
        lex_resources_words_bulk = []
        for word, resources in self.lex_resources_words.items():
            insertion_id = collection.insert_one({'lemma': word}).inserted_id
            for res in resources:
                update_operation = UpdateOne(
                    {'_id': ObjectId(insertion_id)},
                    {'$push': {'resources': {'$ref': 'LexResources', '$id': res}}}
                )
                lex_resources_words_bulk.append(update_operation)

        if len(lex_resources_words_bulk) > 0:
            collection.bulk_write(lex_resources_words_bulk)
        end = time.time()
        print(end - start)

    def create_twitter_table(self):
        db = self.conn
        collection = db.Twitter
        collection.drop()
        start = time.time()
        tweets_bulk = []
        for feeling, words in self.tweets.items():
            tweet_document = {
                'sentiment': feeling,
                'doc_number': enum_feeling[feeling],
                'emoji': self.emoji[feeling],
                'hashtag': self.hashtag[feeling],
                'words': []
            }
            insertion_id = collection.insert_one(tweet_document).inserted_id

            for word, freq in words.items():
                tweet_document['words'].append({
                    'lemma': word,
                    'pos': self.word_pos[word],
                    'freq': freq,
                    'in_lex_resources': {'$ref': 'LexResourcesWords', '$id': word}
                })

            update_operation = UpdateOne({'_id': ObjectId(insertion_id)}, {'$set': {'words': tweet_document['words']}})
            tweets_bulk.append(update_operation)

        if len(tweets_bulk) > 0:
            collection.bulk_write(tweets_bulk)
        end = time.time()
        print(end - start)


import sys
import time
from pprint import pprint

from bson import ObjectId
from pymongo import UpdateOne

class MongoPopulation:

    def __init__(self, mongo_conn, lex_resources, lex_resources_words, resources, tweets, emoji, hashtag, word_pos, emoticons):
        self.lex_resources = lex_resources
        self.lex_resources_words = lex_resources_words
        self.resources = resources
        self.tweets = tweets
        self.emoji = emoji
        self.hashtag = hashtag
        self.conn = mongo_conn
        self.word_pos = word_pos
        self.emoticons = emoticons
        self.support = {}
        self.id_support = {}
        # self.empty_collection()
        # self.create_lex_resources_table()
        # self.create_lex_resources_words_table()
        self.create_id_support()
        self.create_support()
        self.create_twitter_table()

    def empty_collection(self):
        db = self.conn
        twitter = db.Twitter6
        lex_resources = db.LexResources3
        lex_resources_words = db.LexResourcesWords2

        try:
            twitter.delete_many({})
            # lex_resources.delete_many({})
            # lex_resources_words.delete_many({})
        except Exception as e:
            print(f"Error emptying collection: {e}")

    def create_id_support(self):
        db = self.conn
        collection = db.LexResourcesWords2
        collection.create_index([('lemma', 1)])

        all = collection.find()
        for word in all:
            self.id_support[word['lemma']] = word['_id']

    def create_lex_resources_table(self):
        db = self.conn
        collection = db.LexResources3
        start = time.time()
        lex_resources_bulk = []
        for res, values in self.lex_resources.items():
            lex_resources_bulk.append({
                '_id': res,
                'sentiment': values['sentiment'],
                'totNumberWords': values['totNumberWords']
            })

        collection.insert_many(lex_resources_bulk)
        end = time.time()
        print(f"LexResources created in: {end - start}")

    def create_lex_resources_words_table(self):
        db = self.conn
        start = time.time()
        collection = db.LexResourcesWords2
        collection.create_index([('lemma', 1)])

        lex_resources_words_bulk = []
        for word, resources in self.lex_resources_words.items():
            db_refs = [{'$ref': 'LexResources', '$id': resource_id} for resource_id in resources]
            update_operation = UpdateOne(
                {'lemma': word},
                {'$addToSet': {'resources': {'$each': db_refs}}},
                upsert=True
            )
            lex_resources_words_bulk.append(update_operation)

        if lex_resources_words_bulk:
            collection.bulk_write(lex_resources_words_bulk, ordered=False)

        end = time.time()
        print(f"LexResourcesWords created in: {end - start}")

    def create_twitter_table(self):
        db = self.conn
        collection = db.Twitter6
        bulk_operations = []

        start = time.time()

        for feeling, lines in self.tweets.items():
            sentiment_doc = {
                'sentiment': feeling,
                'content': []
            }
            id = collection.insert_one(sentiment_doc).inserted_id

            for line, line_data in lines.items():
                hashtag_list = []
                for word, count in self.hashtag[feeling][line].items():
                    for _ in range(count):
                        hashtag_list.append({word: 1})
                emoji_list = []
                for emoji, count in self.emoji[feeling][line].items():
                    for _ in range(count):
                        emoji_list.append({emoji: 1})
                emoticons_list = []
                for emoticon, count in self.emoticons[feeling][line].items():
                    for _ in range(count):
                        emoticons_list.append({emoticon: 1})
                doc = {
                    'doc_number': line,
                    'words': list(self.support[line]),
                    'hashtags': hashtag_list if len(self.hashtag[feeling][line]) else None,
                    'emojis': emoji_list if len(self.emoji[feeling][line]) else None,
                    'emoticons': emoticons_list if len(self.emoticons[feeling][line]) else None
                }
                # if 1 <= line <= 20000:
                # if 20001 <= line <= 40000:
                if 40001 <= line <= 60000:
                     bulk_operations.append(UpdateOne({'_id': id}, {'$push': {'content': doc}}))

        # Execute bulk insert and update operations
        collection.bulk_write(bulk_operations)

        end = time.time()
        print(f"Twitter created in: {end - start}")

    def create_support(self):
        for feeling, lines in self.tweets.items():
            for line, words in lines.items():
                self.support[line] = []
                for word, count in words.items():
                    id = self.id_support[word] if word in self.id_support else None
                    self.support[line].append({
                        'lemma': word,
                        'count': count,
                        'pos': self.word_pos[word],
                        'in_lex_resources': {'$ref': 'LexResourcesWords', '$id': self.id_support[word]} if id else None,
                    })
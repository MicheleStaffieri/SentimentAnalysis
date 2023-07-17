import time
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
        self.create_lex_resources_table()
        self.create_lex_resources_words_table()
        self.create_twitter_table()

    def create_lex_resources_table(self):
        db = self.conn
        collection = db.LexResources
        collection.drop()
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
        collection = db.LexResourcesWords
        collection.drop()
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
            collection.bulk_write(lex_resources_words_bulk)

        end = time.time()
        print(f"LexResourcesWords created in: {end - start}")

    def create_twitter_table(self):
        db = self.conn
        collection = db.Twitter
        collection.drop()

        start = time.time()
        tweets_bulk = []
        lex_resources_map = {}

        unique_words = list(set(word for words in self.tweets.values() for word in words))

        lex_resource_words = db.LexResourcesWords.find({'lemma': {'$in': unique_words}})
        for lex_resource_word in lex_resource_words:
            lex_resources_map[lex_resource_word['lemma']] = lex_resource_word

        for feeling, words in self.tweets.items():
            tweet_document = {
                'sentiment': feeling,
                '$set': [{
                    'doc_number': 1,
                    'emoji': self.emoji[feeling],
                    'hashtags': self.hashtag[feeling],
                    'words': []
                    }
                ]
            }

            for word, freq in words.items():
                lex_resource_word = lex_resources_map.get(word)
                id_ref = lex_resource_word['_id'] if lex_resource_word else None
                tweet_document['words'].append({
                    'lemma': word,
                    'POS': self.word_pos[word],
                    'freq': freq,
                    'in_lex_resources': {'$ref': 'LexResourcesWords', '$id': id_ref}
                })

            tweets_bulk.append(UpdateOne({'sentiment': feeling}, {'$set': tweet_document}, upsert=True))

        if len(tweets_bulk) > 0:
            collection.bulk_write(tweets_bulk, ordered=False)

        end = time.time()
        print(f"Twitter created in: {end - start}")
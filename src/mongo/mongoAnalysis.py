import time
from pprint import pprint

from bson import DBRef
from pymongo import MongoClient
from tqdm import tqdm
from wordcloud import WordCloud

from src.utils.feeling_list import feeling_list


class MongoAnalysis:

    def __init__(self, mongo_conn):
        self.conn = mongo_conn
        self.calculate_word_statistics()

    def calculate_word_statistics(self):
        db = self.conn
        collection = db.Twitter
        pipeline = [
            {
                '$unwind': '$words'
            },
            {
                '$project': {
                    '_id': 0,
                    'lemma': '$words.lemma',
                    'frequency': '$words.freq',
                    'sentiment': 1,
                }
            },
            {
                '$group': {
                    '_id': '$sentiment',
                    'words': {
                        '$push': {
                            'lemma': '$lemma',
                            'frequency': '$frequency'
                        }
                    }
                }
            }
        ]
        cursor = collection.aggregate(pipeline)
        wordcloud_struc = {}
        for sentiment in cursor:
            wordcloud_struc[sentiment['_id']] = {}
            for word in sentiment['words']:
                pprint(f"{sentiment['_id']}, {word['lemma']} {word['frequency']}")
                wordcloud_struc[sentiment['_id']][word['lemma']] = int(word['frequency'])

        for feeling in tqdm(feeling_list):
            wordcloud_words = WordCloud(max_font_size=50, background_color="white", width=800,height=400).generate_from_frequencies(wordcloud_struc[feeling])
            wordcloud_words.to_file(f"./newResources/WordClouds_mongo/{feeling}/cloud_words_" + feeling + ".png")



if __name__ == '__main__':
    mongo_conn = MongoClient(host='mongodb://127.0.0.1:27117,127.0.0.1:27118').maadb_project_db
    MongoAnalysis(mongo_conn)

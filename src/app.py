from pprint import pprint

from src.NLPAnalysis.nlp import NLPAnalyzer
from src.postgres.PGPopulation import PGPopulation
from src.postgres.PGConnection import PGConnection
from src.postgres.PGAnalysis import PGAnalysis
from src.mongo.mongoConnection import MongoConnection
from src.mongo.mongoPopulation import MongoPopulation
from src.mongo.mongoAnalysis import MongoAnalysis

if __name__ == '__main__':
    nlp = NLPAnalyzer()
    # ask witch db to use in input

    input_choice = input("Insert 1 for postgres or 2 for mongo: ")

    if input_choice == "1":
        pprint('You choose postgres')
        pg_conn = PGConnection().conn
        PGPopulation(pg_conn, nlp.resources, nlp.tweets, nlp.emoji, nlp.tags)
        PGAnalysis(pg_conn)

    elif input_choice == "2":
        pprint('You choose mongo')
        mongo_conn = MongoConnection().mongo_conn
        MongoPopulation(mongo_conn, nlp.lex_resources, nlp.lex_resources_words, nlp.resources, nlp.tweets, nlp.emoji, nlp.tags, nlp.word_pos)
        # MongoAnalysis(mongo_conn)







from pprint import pprint

from src.NLPAnalysis.nlp import NLPAnalyzer
from src.postgreSQL.PGPopulation import PGPopulation
from src.postgreSQL.PGConnection import PGConnection
from src.postgreSQL.PGAnalysis import PGAnalysis
from src.mongoDB.mongoConnection import MongoConnection
from src.mongoDB.mongoPopulation import MongoPopulation
from src.mongoDB.mongoAnalysis import MongoAnalysis

if __name__ == '__main__':
    nlp = NLPAnalyzer()
    pprint('You choose postgreSQL')
    pg_conn = PGConnection().conn
    PGPopulation(pg_conn, nlp.resources, nlp.tweets_pg, nlp.emoji_pg, nlp.tags_pg, nlp.emoticons_pg)
    PGAnalysis(pg_conn)
    # pprint('You choose mongoDB')
    # mongo_conn = MongoConnection().mongo_conn
    # MongoPopulation(mongo_conn, nlp.lex_resources, nlp.lex_resources_words, nlp.resources, nlp.tweets_mongo,
    #                 nlp.emojis_mongo, nlp.tags_mongo, nlp.word_pos, nlp.emoticons_mongo)
    # MongoAnalysis(mongo_conn)
    # input_choice = input("Insert 1 for postgreSQL or 2 for mongoDB: ")
    #
    # while input_choice != "1" and input_choice != "2":
    #     input_choice = input("Wrong input, insert 1 for postgreSQL or 2 for mongoDB: ")
    #
    # if input_choice == "1":
    #     pprint('You choose postgreSQL')
    #     pg_conn = PGConnection().conn
    #     PGPopulation(pg_conn, nlp.resources, nlp.tweets_pg, nlp.emoji_pg, nlp.tags_pg, nlp.emoticons_pg)
    #     PGAnalysis(pg_conn)
    #
    # elif input_choice == "2":
    #     pprint('You choose mongoDB')
    #     mongo_conn = MongoConnection().mongo_conn
    #     MongoPopulation(mongo_conn, nlp.lex_resources, nlp.lex_resources_words, nlp.resources, nlp.tweets_mongo,
    #                     nlp.emojis_mongo, nlp.tags_mongo, nlp.word_pos)
    #     MongoAnalysis(mongo_conn)





from src.NLPAnalysis.nlp import NLPAnalyzer
from src.postgres.PGPopulation import PGPopulation
from src.postgres.PGConnection import PGConnection
from src.postgres.PGAnalysis import PGAnalysis
from src.mongo.mongoConnection import MongoConnection
from src.mongo.mongoPopulation import MongoPopulation
from src.mongo.mongoAnalysis import MongoAnalysis

if __name__ == '__main__':
    nlp = NLPAnalyzer()

    pg_conn = PGConnection().conn
    PGPopulation(pg_conn, nlp.resources, nlp.tweets, nlp.emoji, nlp.tags)
    PGAnalysis(pg_conn)

    mongo_conn = MongoConnection().mongo_conn
    MongoPopulation(mongo_conn, nlp.resources, nlp.tweets, nlp.emoji, nlp.tags)
    MongoAnalysis(mongo_conn)







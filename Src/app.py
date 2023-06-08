from nlp import NLPAnalyzer
from PGPopulation import PGPopulation
from PGConnection import PGConnection
from PGAnalysis import PGAnalysis

if __name__ == '__main__':
    nlp = NLPAnalyzer()
    pgconn = PGConnection()
    PGPopulation(pgconn.conn, nlp.resources, nlp.tweets, nlp.emoji, nlp.tags)
    PGAnalysis(pgconn.conn)





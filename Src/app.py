from nlp import NLPAnalyzer
from postgresConnection import PGConnection

if __name__ == '__main__':
    nlp = NLPAnalyzer()
    pgconn = PGConnection(nlp.resources, nlp.tweets)


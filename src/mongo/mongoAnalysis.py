class MongoAnalysis:

    def __init__(self, mongo_conn):
        self.conn = mongo_conn
        self.emojis_table = {}
        self.hashtags_table = {}
        self.tweets_table = {}
        self.resources_table = {}
        self.intersection = {}
        self.new_words = {}
        self.new_emojis = {}
        self.get_table()
        self.calculate_map_reduce()

    def get_table(self):
        pass

    def calculate_map_reduce(self):
        pass
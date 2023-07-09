class MongoPopulation:

    def __init__(self, mongo_conn, resources, tweets, emoji, hashtag):
        self.resources = resources
        self.tweets = tweets
        self.emoji = emoji
        self.hashtag = hashtag
        self.conn = mongo_conn
        self.create_resources_table()
        self.create_twitter_table()
        self.create_hashtag_table()
        self.create_emoji_table()

    def create_resources_table(self):
        pass

    def create_twitter_table(self):
        pass

    def create_hashtag_table(self):
        pass

    def create_emoji_table(self):
        pass
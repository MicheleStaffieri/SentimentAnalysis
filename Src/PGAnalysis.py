from tqdm import tqdm

from Utils.config import feeling_list
from wordcloud import WordCloud


class PGAnalysis:

    def __init__(self, pgconn):
        self.conn = pgconn
        self.emojis_table = {}
        self.hashtags_table = {}
        self.tweets_table = {}
        self.resources_table = {}

        self.get_table()

        self.wordCloudGen()

    def get_table(self):
        cur = self.conn.cursor()
        for feeling in feeling_list:
            cur.execute(f"SELECT word, w_count FROM emoji_{feeling}")
            self.emojis_table[feeling] = dict(cur.fetchall())

            cur.execute(f"SELECT word, w_count FROM hashtag_{feeling}")
            self.hashtags_table[feeling] = dict(cur.fetchall())

            cur.execute(f"SELECT word, w_count FROM tweet_{feeling}")
            self.tweets_table[feeling] = dict(cur.fetchall())

            cur.execute(f"SELECT word, w_count FROM resources_{feeling}")
            self.resources_table[feeling] = dict(cur.fetchall())

    def wordCloudGen(self):
        for feeling in tqdm(feeling_list):
            wordcloud_words = WordCloud(max_font_size=50, background_color="white", width=800,
                                        height=400).generate_from_frequencies(self.tweets_table[feeling])

            wordcloud_emoji = WordCloud(max_font_size=50, background_color="white", width=800,
                                        height=400).generate_from_frequencies(self.emojis_table[feeling])
            wordcloud_tag = WordCloud(max_font_size=50, background_color="white", width=800,
                                      height=400).generate_from_frequencies(self.hashtags_table[feeling])
            wordcloud_words.to_file("WordClouds/cloud_words_" + feeling + ".png")
            wordcloud_emoji.to_file("WordClouds/cloud_emoji_" + feeling + ".png")
            wordcloud_tag.to_file("WordClouds/cloud_tag_" + feeling + ".png")

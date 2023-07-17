import os
from pprint import pprint

from tqdm import tqdm

from src.postgres.PGConnection import PGConnection
from src.utils.feeling_list import feeling_list
from wordcloud import WordCloud


class PGAnalysis:

    def __init__(self, pgconn):
        self.conn = pgconn
        self.emojis_table = {}
        self.hashtags_table = {}
        self.tweets_table = {}
        self.resources_table = {}
        self.intersection = {}
        self.new_words = {}
        self.new_emojis = {}
        self.get_table()
        self.wordCloudGen()
        self.calculate_intersections()

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
            pprint(f"{feeling}, {self.tweets_table[feeling]}")
            wordcloud_words = WordCloud(max_font_size=50, background_color="white", width=800,
                                        height=400).generate_from_frequencies(self.tweets_table[feeling])

            wordcloud_emoji = WordCloud(max_font_size=50, background_color="white", width=800,
                                        height=400).generate_from_frequencies(self.emojis_table[feeling])
            wordcloud_tag = WordCloud(max_font_size=50, background_color="white", width=800,
                                      height=400).generate_from_frequencies(self.hashtags_table[feeling])
            wordcloud_words.to_file(f"./newResources/WordClouds_pg/{feeling}/cloud_words_" + feeling + ".png")
            wordcloud_emoji.to_file(f"./newResources/WordClouds_pg/{feeling}/cloud_emoji_" + feeling + ".png")
            wordcloud_tag.to_file(f"./newResources/WordClouds_pg/{feeling}/cloud_tag_" + feeling + ".png")

    def calculate_intersections(self):
        RES_PATH = './utils/resources/Risorse lessicali/Archive_risorse_lessicali/'
        cur = self.conn.cursor()
        for feeling in tqdm(feeling_list):
            self.intersection[feeling] = {}
            list_words_for_resource = set()
            list_words_for_resource.clear()
            self.new_words[feeling] = dict()
            for file_resource in os.listdir(RES_PATH + feeling):
                with open(RES_PATH + feeling + "/" + file_resource, 'r') as file:
                    resource_name = file_resource.split('_')[0]
                    cur.execute(f"SELECT word, w_count FROM resources_{feeling} WHERE {resource_name}>0")
                    res_words = dict(cur.fetchall()).keys()
                    for word in res_words:
                        list_words_for_resource.add(word)

                    self.intersection[feeling][resource_name] = [word for word in res_words if
                                                                 word in self.tweets_table[feeling].keys()]

                    perc_presence_lex_rex = (len(self.intersection[feeling][resource_name]) / len(res_words)) * 100
                    perc_presence_twitter = (len(self.intersection[feeling][resource_name]) / len(
                        self.tweets_table[feeling])) * 100
                    # TODO: printResults()
            self.new_words[feeling] = {}
            new_words_resource = open('./newResources/NewWords/new_words_resource_' + feeling + '.txt', 'w')
            new_words_resource.write(f'{feeling.upper()}\n\n')
            for word, count in self.tweets_table[feeling].items():
                if word not in list_words_for_resource:
                    self.new_words[feeling][word] = count
                    new_words_resource.write(f'{word} {count}\n')


if __name__ == '__main__':
    pgconn = PGConnection()
    analysis = PGAnalysis(pgconn.conn)

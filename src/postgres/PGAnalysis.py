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
        self.new_emojis= {}
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
        folder = '../newResources/'
        for feeling in tqdm(feeling_list):
            wordcloud_words = WordCloud(max_font_size=50, background_color="white", width=800,
                                        height=400).generate_from_frequencies(self.tweets_table[feeling])

            wordcloud_emoji = WordCloud(max_font_size=50, background_color="white", width=800,
                                        height=400).generate_from_frequencies(self.emojis_table[feeling])
            wordcloud_tag = WordCloud(max_font_size=50, background_color="white", width=800,
                                      height=400).generate_from_frequencies(self.hashtags_table[feeling])
            wordcloud_words.to_file(f"{folder}WordClouds/{feeling}/cloud_words_" + feeling + ".png")
            wordcloud_emoji.to_file(f"{folder}WordClouds/{feeling}/cloud_emoji_" + feeling + ".png")
            wordcloud_tag.to_file(f"{folder}WordClouds/{feeling}/cloud_tag_" + feeling + ".png")

    def calculate_intersections(self):
        RES_PATH= '../utils/resources/Risorse lessicali/Archive_risorse_lessicali/'
        folder = '../newResources/'
        cur = self.conn.cursor()
        for feeling in tqdm(feeling_list):
            self.intersection[feeling] = {}
            list_words_for_resource = set()
            self.new_words[feeling] = dict()
            for file_feeling in os.listdir(RES_PATH + feeling):
                with open(RES_PATH + feeling + "/" + file_feeling, 'r') as file:
                    resource_name = file_feeling.split('_')[0]
                    cur.execute(f"SELECT word, w_count FROM resources_{feeling} WHERE {resource_name}>0")
                    res_words = dict(cur.fetchall()).keys()
                    for word in res_words:
                        list_words_for_resource.add(word)
                    intersection = [word for word in res_words if word in self.tweets_table[feeling].keys()]
                    self.intersection[feeling][resource_name] = intersection
                    perc_presence_lex_rex = (len(intersection) / len(res_words))*100
                    perc_presence_twitter = (len(intersection) / len(self.tweets_table[feeling]))*100
                    pprint(
                        f'Numero parole condivise fra i tweet di {feeling} e la risorsa {resource_name}= {len(intersection)}')
                    pprint(
                        f'Perc di parole nei tweet di {feeling}, della risorsa {resource_name}= {perc_presence_lex_rex} %')
                    pprint(
                        f'Perc di parole della risorsa {resource_name} nei tweet di {feeling}= {perc_presence_twitter} %')
            self.new_words[feeling] = {}
            new_words_resource = open('../newResources/NewWords/new_words_resource_'+feeling+'.txt', 'w')
            for word, count in self.tweets_table[feeling].items():
                if word not in list_words_for_resource:
                    self.new_words[feeling][word] = count
                    new_words_resource.write(f'{word}   {count}\n')

            # pprint(f'parole nuove trovate: {(self.new_words[feeling])}')
            # pprint(f'Ne ho trovate: {len(self.new_words[feeling])}')



if __name__ == '__main__':
    pgconn = PGConnection()
    analysis = PGAnalysis(pgconn.conn)

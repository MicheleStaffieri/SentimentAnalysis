import os
import time
from pprint import pprint

from matplotlib import pyplot as plt
from tqdm import tqdm

from src.postgreSQL.PGConnection import PGConnection
from src.utils.feeling_list import feeling_list
from wordcloud import WordCloud
import pandas as pd


class PGAnalysis:

    def __init__(self, pgconn):
        self.conn = pgconn
        self.emojis_table = {}
        self.hashtags_table = {}
        self.tweets_table = {}
        self.resources_table = {}
        self.emoticons_table = {}
        self.intersection = {}
        self.new_words = {}
        self.new_emojis = {}
        self.get_table()
        self.wordCloudGen()
        self.calculate_intersections()
        self.histograms()

    def get_table(self):
        cur = self.conn.cursor()
        for feeling in feeling_list:
            start = time.time()
            cur.execute(f"SELECT emoji, em_count FROM emoji_{feeling}")
            self.emojis_table[feeling] = dict(cur.fetchall())

            cur.execute(f"SELECT hashtag, hash_count FROM hashtag_{feeling}")
            self.hashtags_table[feeling] = dict(cur.fetchall())

            cur.execute(f"SELECT word, w_count FROM tweet_{feeling}")
            self.tweets_table[feeling] = dict(cur.fetchall())

            cur.execute(f"SELECT word, w_count FROM resources_{feeling}")
            self.resources_table[feeling] = dict(cur.fetchall())

            cur.execute(f"SELECT emoticon, emo_count FROM emoticons_{feeling}")
            self.emoticons_table[feeling] = dict(cur.fetchall())
            end = time.time()
            print(f"Tables for {feeling} loaded in: {end - start} seconds")

    def wordCloudGen(self):
        for feeling in tqdm(feeling_list):
            wordcloud_words = WordCloud(max_font_size=50, background_color="white", width=800,
                                        height=400).generate_from_frequencies(self.tweets_table[feeling])

            wordcloud_emoji = WordCloud(max_font_size=50, background_color="white", width=800,
                                        height=400).generate_from_frequencies(self.emojis_table[feeling])
            wordcloud_tag = WordCloud(max_font_size=50, background_color="white", width=800,
                                      height=400).generate_from_frequencies(self.hashtags_table[feeling])
            wordcloud_emoticon = WordCloud(max_font_size=50, background_color="white", width=800,
                                           height=400).generate_from_frequencies(self.emoticons_table[feeling])
            wordcloud_words.to_file(f"./newResources/WordClouds_pg/{feeling}/cloud_words_" + feeling + ".png")
            wordcloud_emoji.to_file(f"./newResources/WordClouds_pg/{feeling}/cloud_emoji_" + feeling + ".png")
            wordcloud_tag.to_file(f"./newResources/WordClouds_pg/{feeling}/cloud_tag_" + feeling + ".png")
            wordcloud_emoticon.to_file(f"./newResources/WordClouds_pg/{feeling}/cloud_emoticon_" + feeling + ".png")

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
                    printresult = open('./newResources/PGStats/' + feeling + '_'+ resource_name +'.txt', 'w')
                    printresult.write(f'Percentuale presenza delle parole delle risorse lessicali nei tweet: {perc_presence_lex_rex}\n')
                    printresult.write(f'Percentuale presenza delle parole dei tweet nelle risorse lessicali: {perc_presence_twitter}\n')


            self.new_words[feeling] = {}
            new_words = open('./newResources/NewWords/new_words_' + feeling + '.txt', 'w')
            new_words.write(f'{feeling.upper()}\n\n')

            all_words_resource = open('./newResources/NewLexResources/new_resource_' + feeling + '.txt', 'w')
            all_words_resource.write(f'{feeling.upper()}\n\n')
            new_res = {}
            for word, count in self.tweets_table[feeling].items():
                new_res[word] = count
                if word not in list_words_for_resource:
                    self.new_words[feeling][word] = count
            sorted_new_res = sorted(new_res.items(), key=lambda x: x[1], reverse=True)
            for word, count in sorted_new_res:
                if count > 1:
                    all_words_resource.write(f'{word} {count}\n')

            sorted_new_words = sorted(self.new_words[feeling].items(), key=lambda x: x[1], reverse=True)
            for word, count in sorted_new_words:
                if count > 1:
                    new_words.write(f'{word} {count}\n')

    def histograms(self):
        if feeling_list is None or len(feeling_list) <= 1:
            pprint("Error: feeling_list is empty or has only one element")
        else:
            for feeling in feeling_list:
                labels = [f for f in feeling_list if f != feeling]
                histogram_values = []
                reference = set(self.tweets_table[feeling].keys())
                for other_feeling in feeling_list:
                    if feeling != other_feeling:
                        other_feeling_resources = set(self.resources_table[other_feeling].keys())
                        intersection = list(reference.intersection(other_feeling_resources))
                        histogram_values.append(len(intersection) / len(reference) * 100)
                histogram_values_series = pd.Series(histogram_values)
                plt.figure(figsize=(12, 8))
                ax = histogram_values_series.plot(kind="bar",
                                                  color=['red', 'green', 'blue', 'orange', 'purple', 'brown', 'pink'])
                ax.set_title('Perc of resources in ' + feeling)
                ax.set_xlabel("Amount")
                ax.set_ylabel("Percentage")
                ax.set_xticklabels(labels)
                plt.savefig('./newResources/Histograms/perc_presence_twitter_' + feeling + '.png')


if __name__ == '__main__':
    pgconn = PGConnection()
    analysis = PGAnalysis(pgconn.conn)

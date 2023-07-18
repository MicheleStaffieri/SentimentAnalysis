import os
from pprint import pprint

import nltk
from tqdm import tqdm
import re
from src.utils.emoji import emojiPos, webemoji
from src.utils.emoji import emojiNeg
from src.utils.emoji import othersEmoji
from src.utils.emoji import negemoticons
from src.utils.emoji import posemoticons
from src.utils.slang import slang_words
from src.utils.punctuation import punctuation
from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords
from nltk.corpus import wordnet
from nltk.tokenize import TweetTokenizer
import demoji
import csv
from src.utils.feeling_list import feeling_list

# demoji.download_codes()
# nltk.download('stopwords')
# nltk.download('wordnet')
# nltk.download('averaged_perceptron_tagger')

RES_PATH = "./utils/resources/Risorse lessicali/Archive_risorse_lessicali/"
TWEETS_PATH = "./utils/resources/Twitter messaggi/"


class NLPAnalyzer:

    def __init__(self):
        self.emoji_pg = {}
        self.emojis_mongo = {}
        self.tags_pg = {}
        self.tags_mongo = {}
        self.tweets_pg = {}
        self.tweets_mongo = {}
        self.resources = {}
        self.words_dict = {}
        self.lex_resources = {}
        self.lex_resources_words = {}
        self.word_pos = {}

        self.afinn_score = {}
        self.anew_score = {}
        self.dal_score = {}


        self.analyze_tweets()
        self.create_resources_dictionary()



    def pos_tagging(self, word_tokens):
        sw = set(stopwords.words('english'))
        word_tokens = [w for w in word_tokens if (w not in sw)]

        tag_dict = {"J": wordnet.ADJ,
                    "N": wordnet.NOUN,
                    "V": wordnet.VERB,
                    "R": wordnet.ADV}
        res = []
        ts = nltk.pos_tag(word_tokens)
        for t in ts:
            res.append((t[0], tag_dict.get(str(t[1][0]).upper(), 'q')))
        return res

    def analyze_tweets(self):
        tag_list = {}
        emoji_list = {}
        lemmatized_tweets = {}
        tk = TweetTokenizer()
        lemmatizer = WordNetLemmatizer()

        for feeling in tqdm(feeling_list):
            lemmatized_tweets[feeling] = {}
            tag_list[feeling] = {}
            emoji_list[feeling] = {}
            self.tweets_mongo[feeling] = {}
            self.emojis_mongo[feeling] = {}
            self.tags_mongo[feeling] = {}
            with open(TWEETS_PATH + "dataset_dt_" + feeling.lower() + "_60k.txt", 'r', encoding="utf8") as file:
                lines = file.readlines()
                print("Start Analyzing tweet. Feeling: ", feeling)
                line_number = 1

                for line in lines:

                    self.tweets_mongo[feeling][line_number] = {}
                    self.emojis_mongo[feeling][line_number] = {}
                    self.tags_mongo[feeling][line_number] = {}

                    # rimozione delle parole che coprono gli username e gli url
                    line = line.replace('USERNAME', '').replace('URL', '').lower()

                    # salvataggio degli hashtag come parola che contengono, e rimozione del cancelletto
                    if '#' in line:
                        hashtags = re.findall(r"#(\w+)", line)
                        for htag in hashtags:
                            self.tags_mongo[feeling][line_number][htag] = self.tags_mongo[feeling][line_number].get(htag, 0) + 1
                            tag_list[feeling][htag] = tag_list[feeling].get(htag, 0) + 1
                            line = line.replace('#' + htag, '').replace('#', '')

                    # salvataggio del corpus di emoji come descrizione del concetto che veicolano
                    # demoji le sostituisce con una descrizione a parole delimitata dal carattere ':' 
                    ejs = [[demoji.replace_with_desc(em, ":"), em] for em in
                           emojiNeg + emojiPos + othersEmoji + negemoticons +
                           posemoticons + webemoji if (em in line)]

                    # dopodiché per ogni emoji trovata nei tweet e decodificata a parole,
                    # la processiamo contandone l'occorrenza in un dizionario 
                    for dem, em in ejs:
                        self.emojis_mongo[feeling][line_number][dem] = self.emojis_mongo[feeling][line_number].get(dem, 0) + 1
                        emoji_list[feeling][dem] = emoji_list[feeling].get(dem, 0) + 1
                        line = line.replace(em, ' ')

                    # processing della punteggiatura
                    punct_list = [p for p in punctuation if (p in line)]
                    for p in punct_list:
                        line = line.replace(p, ' ')

                    # processing dello slang: ogni espressione identificata come slang viene sostituita
                    # con il proprio significato per intero, ottenuto dal dizionario in slang.py
                    slang_list = [s for s in slang_words.keys() if (s in line.split())]
                    for s in slang_list:
                        line = line.replace(s, slang_words[s])
                    # tokenizzazione
                    word_tokens = tk.tokenize(line)
                    # tokenizzazione in part of speech e lemmatizzazione delle parole
                    pos_line = self.pos_tagging(word_tokens)
                    for pos in pos_line:
                        if pos[1] in ['j', 'n', 'v', 'r']:
                            lemm_w = lemmatizer.lemmatize(pos[0], pos[1])
                            if lemm_w.encode('unicode-escape').startswith(b'\\u'):
                                continue
                            lemmatized_tweets[feeling][lemm_w] = lemmatized_tweets[feeling].get(lemm_w, 0) + 1

                            self.tweets_mongo[feeling][line_number][lemm_w] = self.tweets_mongo[feeling][line_number].get(lemm_w, 0) + 1
                            self.word_pos[lemm_w] = pos[1]
                line_number += 1
            # salvataggio delle strutture dati globali: per ciascuno degli 8 sentimenti,
            # una entry di dizionario per le emoji raccolte, una per i lemmi, una per i tag trovati
            self.emoji_pg[feeling] = emoji_list[feeling]
            self.tweets_pg[feeling] = lemmatized_tweets[feeling]
            self.tags_pg[feeling] = tag_list[feeling]

    # questa è la funzione riadattata ai nostri dizionari e con il self
    # final structure: {Emotion: {word: {count, NRC, EmoSN, sentisensem, afinn, anew, del},...}
    def create_resources_dictionary(self):
        self.create_afinn_anew_dal()
        for feeling in feeling_list:
            list_words = {}
            for file_feeling in os.listdir(RES_PATH + feeling):
                with open(RES_PATH + feeling + "/" + file_feeling, 'r') as file:
                    lines = file.readlines()
                    resource_name = file_feeling.split('_')[0]
                    resource_file = file_feeling.split('.')[0]
                    self.lex_resources[resource_file] = {
                        'sentiment': feeling,
                        'totNumberWords': len(lines)
                    }
                    for line in lines:
                        if '_' not in line:
                            key = line.replace('\n', "")
                            # la parola key può gia essere presente o no come *chiave* del dizionario
                            if key not in list_words:
                                self.lex_resources_words[key] = [resource_file]
                                list_words[key] = {'afinn': self.afinn_score.get(key, 0),
                                                   'anewAro': self.anew_score.get(key, 0),
                                                   'dalActiv': self.dal_score.get(key, 0),
                                                   'count': 1,
                                                   resource_name: 1}
                            else:
                                self.lex_resources_words[key].append(resource_file)
                                list_words[key].update({resource_name: 1})
                                list_words[key]['count'] += 1
            self.resources[feeling] = list_words

    def create_afinn_anew_dal(self):
        # Affin
        path = RES_PATH + "ConScore"
        tsv_file = open(path + "/afinn.txt", 'r')
        read_tsv = csv.reader(tsv_file, delimiter="\t")
        for row in read_tsv:
            self.afinn_score[row[0]] = row[1]
        # ANEW
        tsv_file = open(path + "/anewAro_tab.tsv", 'r')
        read_tsv = csv.reader(tsv_file, delimiter="\t")
        for row in read_tsv:
            self.anew_score[row[0]] = row[1]
        # DAL
        tsv_file = open(path + "/Dal_Activ.csv", 'r')
        read_tsv = csv.reader(tsv_file, delimiter="\t")
        for row in read_tsv:
            self.dal_score[row[0]] = row[1]

import os
import time
from pprint import pprint

import nltk
import re

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

RES_PATH = "./utils/resources/Risorse lessicali/Archive_risorse_lessicali/"
TWEETS_PATH = "./utils/resources/Twitter messaggi/"

class NLPAnalyzer:

    def __init__(self):
        self.emoji_pg = {}
        self.emojis_mongo = {}
        self.emoticons_pg = {}
        self.emoticons_mongo = {}
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

    #processing dei tweet e salvataggio nelle strutture dati che andranno a comporre e popolare le basi dati
    def analyze_tweets(self):
        main_start = time.time()
        tag_list = {}
        emoji_list = {}
        emoticons_list = {}
        lemmatized_tweets = {}
        tk = TweetTokenizer()
        lemmatizer = WordNetLemmatizer()

        for feeling in feeling_list:

            #inizializzazione dei dizionari 
            feeling_start = time.time()
            lemmatized_tweets[feeling] = {}
            tag_list[feeling] = {}
            emoji_list[feeling] = {}
            emoticons_list[feeling] = {}
            self.tweets_mongo[feeling] = {}
            self.emojis_mongo[feeling] = {}
            self.tags_mongo[feeling] = {}
            self.emoticons_mongo[feeling] = {}

            #processing riga per riga dei messaggi tweet
            with open(TWEETS_PATH + "dataset_dt_" + feeling.lower() + "_60k.txt", 'r', encoding="utf8") as file:
                lines = file.readlines()
                print("Start Analyzing tweet. Feeling: ", feeling)
                line_number = 1
                for line in lines:
                    self.tweets_mongo[feeling][line_number] = {}
                    self.emojis_mongo[feeling][line_number] = {}
                    self.tags_mongo[feeling][line_number] = {}
                    self.emoticons_mongo[feeling][line_number] = {}

                    # rimozione delle parole che coprono gli username e gli url
                    line = line.replace('USERNAME', '').replace('URL', '').lower()

                    # salvataggio degli hashtag come parola che contengono, e rimozione del cancelletto
                    if '#' in line:
                        hashtags = re.findall(r"#(\w+)", line)
                        for htag in hashtags:
                            self.tags_mongo[feeling][line_number][htag] = self.tags_mongo[feeling][line_number].get(htag, 0) + 1
                            tag_list[feeling][htag] = tag_list[feeling].get(htag, 0) + 1
                            line = line.replace('#' + htag, '').replace('#', '')

                    # salvataggio delle emoji e delle emoticons: tutte le emoticons in una riga vengono salvate
                    # in una lista temporanea e poi nei dizionari per creare le basi dati.
                     
                    emoticon_dataset = posemoticons + negemoticons
                    emoticons = []
                    for word in line.split(' '):
                        if word in emoticon_dataset:
                            emoticons.append(word)

                    # le emoji vengono annotate in una lista temporanea e in un dizionario:
                    # la lista mantiene anche le occorrenze multiple in una riga, il dizionario mantiene il mapping
                    # fra l'emoji e la sua demojizzazione. Vengono poi salvate nei dizionari per creare le basi dati
                    emojis = demoji.findall_list(line)
                    emoji_dict = demoji.findall(line)
                    emoji_rev = {v: k for k, v in emoji_dict.items()}

                    for em in emoticons:
                        self.emoticons_mongo[feeling][line_number][em] = self.emoticons_mongo[feeling][line_number].get(em, 0) + 1
                        emoticons_list[feeling][em] = emoticons_list[feeling].get(em, 0) + 1
                        line = line.replace(em, ' ')

                    for dem in emojis:
                        self.emojis_mongo[feeling][line_number][dem] = self.emojis_mongo[feeling][line_number].get(dem, 0) + 1
                        emoji_list[feeling][dem] = emoji_list[feeling].get(dem, 0) + 1
                        line = line.replace(emoji_rev[dem], ' ')

                    # processing della punteggiatura
                    punct_list = [p for p in punctuation if (p in line)]
                    for p in punct_list:
                        line = line.replace(p, ' ')

                    # processing dello slang: ogni espressione identificata come slang viene sostituita
                    # con il proprio significato per intero, ottenuto dal dizionario in slang.py che abbiamo esteso
                    # con espressioni che abbiano notato comparire nel corpus di tweet
                    slang_list = [s for s in slang_words.keys() if (s in line.split())]
                    for s in slang_list:
                        line = line.replace(s, slang_words[s])
                    
                    # tokenizzazione
                    word_tokens = tk.tokenize(line)
                    # tokenizzazione in part of speech e lemmatizzazione delle parole
                    # i lemmi vengono salvati nei dizionari che ne segnano anche la frequenza:
                    # questi dizionari andranno a comporre le basi dati
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

            # salvataggio delle strutture dati per popolare la base dati relazionale:
            # un dizionario per le emoji, uno per le parole, uno per gli hashtag e uno per le emoticons.
            # per ciascun dizionario, una entry per sentimento contenente il dizionario 
            self.emoji_pg[feeling] = emoji_list[feeling]
            self.tweets_pg[feeling] = lemmatized_tweets[feeling]
            self.tags_pg[feeling] = tag_list[feeling]
            self.emoticons_pg[feeling] = emoticons_list[feeling]

            feeling_end = time.time()
            print("End Analyzing tweet. Feeling: ", feeling, " Time: ", feeling_end - feeling_start)
        main_end = time.time()
        print("End Analyzing tweet. Time: ", main_end - main_start)

    #creazione delle strutture dati per memorizzare le risorse lessicali sia su Mongo che nel relazionale
    def create_resources_dictionary(self):
        main_start = time.time()
        self.create_afinn_anew_dal()

        for feeling in feeling_list:
            feeling_start = time.time()
            list_words = {}
            for file_feeling in os.listdir(RES_PATH + feeling):
                with open(RES_PATH + feeling + "/" + file_feeling, 'r') as file:
                    lines = file.readlines()

                    # salvataggio del nome della risorsa (split su _) e del nome del file (split su .)
                    resource_name = file_feeling.split('_')[0]
                    resource_file = file_feeling.split('.')[0]
                    self.lex_resources[resource_file] = {
                        'sentiment': feeling,
                        'totNumberWords': len(lines)
                    }
                    for line in lines:
                        if '_' not in line:
                            key = line.replace('\n', "")

                            # preparazione al salvataggio in Mongo: come da linee guida, le parole delle risorse lessicali
                            # vengono salvate in un listone che mappa la parola con la lista dei file di risorse in cui questa compare
                            if key not in self.lex_resources_words.keys():
                                self.lex_resources_words[key] = [resource_file]
                            else:
                                self.lex_resources_words[key].append(resource_file)

                            # preparazione al salvataggio in relazionale:
                            # la parola key può gia essere presente o no come *chiave* del dizionario
                            # se non presente crea l'entry di dizionario e ne prende gli score,
                            # settando la frequenza a 1 e segnando il nome della risorsa da cui arriva
                            # se già presente ne incrementa la frequenza e segna il nome della risorsa da cui arriva
                            if key not in list_words:
                                list_words[key] = {'afinn': self.afinn_score.get(key, 0),
                                                   'anewAro': self.anew_score.get(key, 0),
                                                   'dalActiv': self.dal_score.get(key, 0),
                                                   'count': 1,
                                                   resource_name: 1}
                            else:
                                list_words[key].update({resource_name: 1})
                                list_words[key]['count'] += 1

            self.resources[feeling] = list_words
            feeling_end = time.time()
            print("Resources for feeling: ", feeling, "analyzed in: ", feeling_end - feeling_start)
        main_end = time.time()
        print("Resources analyzed in: ", main_end - main_start)

    #funzione di supporto che prende gli score numerici delle risorse etichettare con score
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

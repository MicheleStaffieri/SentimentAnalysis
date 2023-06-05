import os
import nltk
from tqdm import tqdm
import re
from pprint import pprint
from Utils.emoji import emojiPos
from Utils.emoji import emojiNeg
from Utils.emoji import othersEmoji
from Utils.emoji import negemoticons
from Utils.emoji import posemoticons
from Utils.slang import slang_words
from Utils.punctuation import punctuation
from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords
from nltk.corpus import wordnet
from nltk.tokenize import TweetTokenizer
import demoji
import csv

demoji.download_codes()
nltk.download('stopwords')
nltk.download('wordnet')
nltk.download('averaged_perceptron_tagger')

RES_PATH = "../Resources/Risorse lessicali/Archive_risorse_lessicali/"
TWEETS_PATH = "../Resources/Twitter messaggi/"
feeling_list = ['Anger', 'Anticipation', 'Disgust', 'Fear', 'Joy', 'Sadness', 'Surprise', 'Trust']


class NLPAnalyzer:
    def __init__(self):
        self.words = {}
        self.emoji = {}
        self.tags = {}
        self.tweets = {}
        self.resources = {}
        self.words_dict = {}

        self.afinn_score = {}
        self.anew_Aro_score = {}
        self.anew_Dom_score = {}
        self.anew_Pleas_score = {}
        self.dal_Activ_score = {}
        self.dal_Imag_score = {}
        self.dal_Pleas_score = {}

        self.analyze_tweets()
        self.analyze_resources()

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
            self.words[feeling] = []
            with open(TWEETS_PATH + "dataset_dt_" + feeling.lower() + "_60k.txt", 'r', encoding="utf8") as file:
                lines = file.readlines()
                print("Start Analyzing tweet. Feeling: ", feeling)
                for line in lines:

                    # rimozione delle parole che coprono gli username e gli url
                    line = line.replace('USERNAME', '').replace('URL', '')

                    # salvataggio degli hashtag come parola che contengono, e rimozione del cancelletto
                    if '#' in line:
                        hashtags = re.findall(r"#(\w+)", line)
                        for htag in hashtags:
                            tag_list[htag] = tag_list.get(htag, 0) + 1
                            line = line.replace('#' + htag, '').replace('#', '')
                            self.words[feeling].append(htag)

                    # salvataggio del corpus di emoji come descrizione del concetto che veicolano
                    # demoji le sostituisce con una descrizione a parole delimitata dal carattere ':' 
                    ejs = [demoji.replace_with_desc(em, ":") for em in
                           emojiNeg + emojiPos + othersEmoji + negemoticons +
                           posemoticons if (em in line)]

                    # dopodiché per ogni emoji trovata nei tweet e decodificata a parole,
                    # la processiamo contandone l'occorrenza in un dizionario 
                    for e in ejs:
                        emoji_list[e] = emoji_list.get(e, 0) + 1
                        line = line.replace(e, '')
                        self.words[feeling].append(e)

                    # processing della punteggiatura
                    punct_list = [p for p in punctuation if (p in line)]
                    for p in punct_list:
                        line = line.replace(p, ' ')

                    # conversione a caratteri minuscoli e tokenizzazione
                    line = line.lower()
                    word_tokens = tk.tokenize(line)

#### non mi torna: ha senso che la sostituzione dello slang avvenga a tokenizzazione già fatta?

                    # processing dello slang: ogni espressione identificata come slang viene sostituita
                    # con il proprio significato per intero, ottenuto dal dizionario in slang.py
                    slang_list = [s for s in slang_words.keys() if (s in line.split())]
                    for s in slang_list:
                        line = line.replace(s, slang_words[s])

                    # tokenizzazione in part of speech e lemmatizzazione delle parole
                    pos_line = self.pos_tagging(word_tokens)
                    for pos in pos_line:
                        if pos[1] in ['j', 'n', 'v']:
                            lemm_w = lemmatizer.lemmatize(pos[0], pos[1])
                            self.words[feeling].append(lemm_w)
                            lemmatized_tweets[lemm_w] = lemmatized_tweets.get(lemm_w, 0) + 1

            # salvataggio delle strutture dati globali: per ciascuno degli 8 sentimenti,
            # una entry di dizionario per le emoji raccolte, una per i lemmi, una per i tag trovati
            self.emoji[feeling] = emoji_list
            self.tweets[feeling] = lemmatized_tweets
            self.tags[feeling] = tag_list

    # pre-processing delle risorse lessicali
    def analyze_resources(self):
        for feeling in tqdm(feeling_list):
            for feeling_resource in os.listdir(RES_PATH + feeling):

#### non mi torna: serve davvero questo if?
                if feeling_resource.endswith(".txt"):
                    with open(RES_PATH + feeling + "/" + feeling_resource, 'r', encoding="utf8") as file:
                        lines = file.readlines()

                        #
                        for line in lines:
                            if '_' not in line:
                                line = line.replace('\n', '')
                                


##################################################################################################
# questa è la funzione riadattata ai nostri dizionari e con il self

# final structure: {Emotion: {word: {count, NRC, EmoSN, sentisensem, afinn, anew, del},...}
    def create_resources_dictionary(self):
        list_words = {}
        self.create_afinn_anew_dal()
        for feeling in feeling_list:
            for file_feeling in os.listdir(RES_PATH + feeling):
                if not file_feeling.startswith('.'):
                    with open(RES_PATH + feeling + "/" + file_feeling, 'r') as file:
                        t = file_feeling.split('_')[0]
                        lines = file.readlines()
                        for line in lines:
                            if '_' not in line:
                                key = line.replace('\n', "")

                                #la parola key può gia essere presente o no come *chiave* del dizionario
                                if key not in list_words:
                                    list_words[key] = {'afinn': self.afinn_score.get(key, 0),
                                                    'anewAro': self.anew_Aro_score.get(key, 0),
                                                    'anewDom': self.anew_Dom_score.get(key, 0),
                                                    'anewPleas': self.anew_Pleas_score.get(key, 0),
                                                    'dalActiv': self.dal_Activ_score.get(key, 0),
                                                    'dalImag': self.dal_Imag_score.get(key, 0),
                                                    'dalPleas': self.dal_Pleas_score.get(key, 0),
                                                    'count': 1,
                                                    t: 1}
                                else:
                                    list_words[key].update({t: 1})
                                    list_words[key]['count'] += 1
        return list_words

    def create_afinn_anew_dal(self):
        # Affin
        tsv_file = open(RES_PATH + "ConScore" + "/afinn.tsv", 'r')
        read_tsv = csv.reader(tsv_file, delimiter="\t")
        for row in read_tsv:
            self.afinn_score[row[0]] = row[1]

        # ANEW_aro
        tsv_file = open(RES_PATH + "ConScore" + "/anewAro_tab.tsv", 'r')
        read_tsv = csv.reader(tsv_file, delimiter="\t")
        for row in read_tsv:
            self.anew_Aro_score[row[0]] = row[1]

        # ANEW_dom
        tsv_file = open(RES_PATH + "ConScore" + "/anewDom_tab.tsv", 'r')
        read_tsv = csv.reader(tsv_file, delimiter="\t")
        for row in read_tsv:
            self.anew_Dom_score[row[0]] = row[1]
        
        #ANEW_please
        tsv_file = open(RES_PATH + "ConScore" + "/anewPleas_tab.tsv", 'r')
        read_tsv = csv.reader(tsv_file, delimiter="\t")
        for row in read_tsv:
            self.anew_Pleas_score[row[0]] = row[1]

        # DAL_activ
        tsv_file = open(RES_PATH + "ConScore" + "/Dal_Activ.csv", 'r')
        read_tsv = csv.reader(tsv_file, delimiter="\t")
        for row in read_tsv:
            self.dal_Activ_score[row[0]] = row[1]
        
        # DAL_imag
        tsv_file = open(RES_PATH + "ConScore" + "/Dal_Imag.csv", 'r')
        read_tsv = csv.reader(tsv_file, delimiter="\t")
        for row in read_tsv:
            self.dal_Imag_score[row[0]] = row[1]

        # DAL_pleas
        tsv_file = open(RES_PATH + "ConScore" + "/Dal_Pleas.csv", 'r')
        read_tsv = csv.reader(tsv_file, delimiter="\t")
        for row in read_tsv:
            self.dal_Pleas_score[row[0]] = row[1]
########################################################################################


if __name__ == '__main__':
    nlp = NLPAnalyzer()
    nlp.analyze_tweets()
    nlp.analyze_resources()

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

demoji.download_codes()
nltk.download('stopwords')
nltk.download('wordnet')
nltk.download('averaged_perceptron_tagger')

res_base_path = "../Resources/Risorse lessicali/Archive_risorse_lessicali/"
tweets_path = "../Resources/Twitter messaggi/"
feeling_list = ['Anger', 'Anticipation', 'Disgust', 'Fear', 'Joy', 'Sadness', 'Surprise', 'Trust']


class NLPAnalyzer:
    def __init__(self):
        self.words = {}
        self.emoji = {}
        self.tags = {}
        self.tweets = {}
        self.resources = {}
        self.analyze_tweets()
        self.analyze_resources()

    def pos_tagging(self, word_tokens):
        # remove stop words
        sw = set(stopwords.words('english'))
        # remove stop words from word_tokens
        word_tokens = [w for w in word_tokens if (w not in sw)]

        tag_dict = {"J": wordnet.ADJ,
                    "N": wordnet.NOUN,
                    "V": wordnet.VERB,
                    "R": wordnet.ADV}

        # Map POS tag to first character lemmatize() accepts
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
            with open(tweets_path + "dataset_dt_" + feeling.lower() + "_60k.txt", 'r', encoding="utf8") as file:
                lines = file.readlines()
                print("Start Analyzing tweet. Feeling: ", feeling)
                for line in lines:

                    line = line.replace('USERNAME', '').replace('URL', '')

                    if '#' in line:
                        hashtags = re.findall(r"#(\w+)", line)
                        for htag in hashtags:
                            tag_list[htag] = tag_list.get(htag, 0) + 1
                            line = line.replace('#' + htag, '').replace('#', '')
                            self.words[feeling].append(htag)

                    ejs = [demoji.replace_with_desc(em, ":") for em in
                           emojiNeg + emojiPos + othersEmoji + negemoticons +
                           posemoticons if (em in line)]

                    for e in ejs:
                        emoji_list[e] = emoji_list.get(e, 0) + 1
                        line = line.replace(e, '')
                        self.words[feeling].append(e)

                    punct_list = [p for p in punctuation if (p in line)]
                    for p in punct_list:
                        line = line.replace(p, ' ')

                    line = line.lower()

                    word_tokens = tk.tokenize(line)
                    # replace slang from sentences
                    slang_list = [s for s in slang_words.keys() if (s in line.split())]
                    for s in slang_list:
                        line = line.replace(s, slang_words[s])

                    pos_line = self.pos_tagging(word_tokens)

                    # lemmatize nouns, adjective, verbs
                    for pos in pos_line:
                        if pos[1] in ['j', 'n', 'v']:
                            lemm_w = lemmatizer.lemmatize(pos[0], pos[1])
                            self.words[feeling].append(lemm_w)
                            lemmatized_tweets[lemm_w] = lemmatized_tweets.get(lemm_w, 0) + 1
            # Store emoji, tags and tweets for feeling
            self.emoji[feeling] = emoji_list
            self.tweets[feeling] = lemmatized_tweets
            self.tags[feeling] = tag_list

    def analyze_resources(self):
        for feeling in tqdm(feeling_list):
            for feeling_resource in os.listdir(res_base_path + feeling):
                if feeling_resource.endswith(".txt"):
                    with open(res_base_path + feeling + "/" + feeling_resource, 'r', encoding="utf8") as file:
                        lines = file.readlines()
                        for line in lines:
                            if not '_' in line:
                                line = line.replace('\n', '')
                                self.resources[feeling, feeling_resource] = self.resources.get(
                                    (feeling, feeling_resource),[]) + [line]


if __name__ == '__main__':
    nlp = NLPAnalyzer()
    nlp.analyze_tweets()
    nlp.analyze_resources()

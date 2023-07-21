import time
from pprint import pprint

import psycopg2

class PGPopulation:

    def __init__(self, pgconn, resources, tweets, emojis, hashtags, emoticons):
        self.resources = resources
        self.tweets = tweets
        self.emojis = emojis
        self.emoticons = emoticons
        self.hashtags = hashtags
        self.conn = pgconn
        self.create_resources_table()
        self.create_twitter_table()
        self.create_hashtag_table()
        self.create_emoji_table()
        self.create_emoticons_table()


    def create_resources_table(self):
        start = time.time()
        cur = self.conn.cursor()
        if self.resources:
            for feeling, w_list in self.resources.items():
                cur.execute(f'DROP TABLE IF EXISTS resources_{feeling} CASCADE')
                cur.execute(
                    f'CREATE TABLE resources_{feeling} ('
                    f'id SERIAL PRIMARY KEY,'
                    f'word varchar(255),'
                    f'w_count float , '
                    f'nrc integer , '
                    f'emosn integer, '
                    f'sentisense integer, '
                    f'afinn real , '
                    f'anew real, '
                    f'dal real)'
                )
                for key, value in w_list.items():
                    cur.execute(
                        f"INSERT INTO resources_{feeling}(word, w_count, nrc, EmoSN, sentisense, afinn, anew, dal)"
                        f"VALUES('{key}',"
                        f" {value['count']}, "
                        f"{value.get('NRC', 0)}, "
                        f"{value.get('EmoSN', 0)},"
                        f"{value.get('sentisense', 0)},"
                        f" {value.get('afinn', 0)},"
                        f"{value.get('anewAro', 0)}, "
                        f"{value.get('dalActiv', 0)})"
                    )
                # cur.execute(f"SELECT * FROM resources_{feeling}")
                # print(cur.fetchall())
        cur.close()
        self.conn.commit()
        end = time.time()
        print(f"Resources created in: {end - start}")

    def create_twitter_table(self):
        start = time.time()
        cur = self.conn.cursor()

        if len(self.tweets) > 0:
            try:
                cur.execute("BEGIN;")  # Start a transaction

                for feeling, w_list in self.tweets.items():
                    cur.execute(f"DROP TABLE IF EXISTS tweet_{feeling} CASCADE")
                    cur.execute(
                        f'CREATE TABLE tweet_{feeling} ('
                        f'id SERIAL PRIMARY KEY,'
                        f'word varchar(255) NOT NULL, '
                        f'w_count integer ,'
                        f'resources_id integer,'
                        f'CONSTRAINT fk_resources FOREIGN KEY(resources_id) REFERENCES resources_{feeling}(id));'
                    )

                    # Prepare data for bulk insert
                    data = [(key.replace("'", ""), value, key.replace("'", "")) for key, value in w_list.items()]
                    cur.executemany(
                        f'INSERT INTO tweet_{feeling}(word, w_count, resources_id) VALUES (%s, %s, (SELECT id FROM resources_{feeling} WHERE word = %s))',
                        data
                    )

                cur.execute("COMMIT;")  # Commit the transaction

            except Exception as e:
                cur.execute("ROLLBACK;")  # Rollback the transaction in case of an error
                print(f"An error occurred: {e}")

            finally:
                cur.close()

        self.conn.commit()
        end = time.time()
        print(f"Twitter created in: {end - start}")


    def create_emoji_table(self):
        start = time.time()
        cur = self.conn.cursor()
        if len(self.emojis) > 0:
            for feeling, w_list in self.emojis.items():
                cur.execute(f"DROP TABLE IF EXISTS emoji_{feeling} CASCADE")
                cur.execute(
                    f'CREATE TABLE emoji_{feeling} ('
                    f'id SERIAL PRIMARY KEY,'
                    f'emoji varchar(255) UNIQUE NOT NULL, '
                    f'em_count integer)'
                )
                for key, value in w_list.items():
                    key = key.replace("\'", "")
                    cur.execute(
                        f'INSERT INTO emoji_{feeling}(emoji, em_count)'
                        f'VALUES(\'{key}\', 1)'
                        f'ON  CONFLICT (emoji) '
                        f'DO UPDATE  SET em_count = emoji_{feeling}.em_count + 1'
                    )
                # cur.execute(f"SELECT * FROM tweet_{feeling}")
                # print(cur.fetchall())
        cur.close()
        self.conn.commit()
        end = time.time()
        print(f"Emoji created in: {end - start}")

    def create_hashtag_table(self):
        start = time.time()
        cur = self.conn.cursor()
        if len(self.hashtags) > 0:
            for feeling, w_list in self.hashtags.items():
                cur.execute(f"DROP TABLE IF EXISTS hashtag_{feeling} CASCADE")
                cur.execute(
                    f'CREATE TABLE hashtag_{feeling} ('
                    f'id SERIAL PRIMARY KEY,'
                    f'hashtag varchar(255) UNIQUE NOT NULL, '
                    f'hash_count integer)'
                )
                for key, value in w_list.items():
                    key = key.replace("\'", "")
                    cur.execute(
                        f'INSERT INTO hashtag_{feeling}(hashtag, hash_count)'
                        f'VALUES(\'{key}\', 1)'
                        f' ON  CONFLICT (hashtag) '
                        f'DO UPDATE  SET hash_count = hashtag_{feeling}.hash_count + 1'
                    )
                # cur.execute(f"SELECT * FROM tweet_{feeling}")
                # print(cur.fetchall())
        cur.close()
        self.conn.commit()
        end = time.time()
        print(f"Hashtag created in: {end - start}")

    def create_emoticons_table(self):
        start = time.time()
        cur = self.conn.cursor()
        if len(self.emoticons) > 0:
            for feeling, w_list in self.emoticons.items():
                cur.execute(f"DROP TABLE IF EXISTS emoticons_{feeling} CASCADE")
                cur.execute(
                    f'CREATE TABLE emoticons_{feeling} ('
                    f'id SERIAL PRIMARY KEY,'
                    f'emoticon varchar(255) UNIQUE NOT NULL, '
                    f'emo_count integer)'
                )
                for key, value in w_list.items():
                    key = key.replace("\'", "")
                    cur.execute(
                        f'INSERT INTO emoticons_{feeling}(emoticon, emo_count)'
                        f'VALUES(\'{key}\', 1)'
                        f' ON  CONFLICT (emoticon) '
                        f'DO UPDATE  SET emo_count = emoticons_{feeling}.emo_count + 1'
                    )
                # cur.execute(f"SELECT * FROM tweet_{feeling}")
                # print(cur.fetchall())
        cur.close()
        self.conn.commit()
        end = time.time()
        print(f"Emoticons created in: {end - start}")




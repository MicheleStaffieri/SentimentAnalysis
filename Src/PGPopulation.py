import psycopg2

class PGPopulation:

    def __init__(self, pgconn, resources, tweets, emoji, hashtag):
        self.resources = resources
        self.tweets = tweets
        self.emoji = emoji
        self.hashtag = hashtag
        self.conn = pgconn
        self.create_resources_table()
        self.create_twitter_table()
        self.create_hashtag_table()
        self.create_emoji_table()

    def create_resources_table(self):
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

    def create_twitter_table(self):
        cur = self.conn.cursor()
        if len(self.tweets) > 0:
            for feeling, w_list in self.tweets.items():
                cur.execute(f"DROP TABLE IF EXISTS tweet_{feeling} CASCADE")
                cur.execute(
                    f'CREATE TABLE tweet_{feeling} ('
                    f'id SERIAL PRIMARY KEY,'
                    f'word varchar(255) UNIQUE NOT NULL, '
                    f'w_count float ,'
                    f'resources_id integer,'
                    f'CONSTRAINT fk_resources FOREIGN KEY(resources_id) REFERENCES resources_{feeling}(id));'
                )
                for key, value in w_list.items():
                    key = key.replace("\'", "")
                    cur.execute(
                        f'INSERT INTO tweet_{feeling}(word, w_count, resources_id)'
                        f'VALUES(\'{key}\', 1,  (SELECT id FROM resources_{feeling} WHERE word = \'{key}\') )'
                        f'ON  CONFLICT (word) '
                        f'DO UPDATE  SET w_count = tweet_{feeling}.w_count + 1'
                    )
                # cur.execute(f"SELECT * FROM tweet_{feeling}")
                # print(cur.fetchall())
        cur.close()
        self.conn.commit()


    def create_emoji_table(self):
        cur = self.conn.cursor()
        if len(self.emoji) > 0:
            for feeling, w_list in self.emoji.items():
                cur.execute(f"DROP TABLE IF EXISTS emoji_{feeling} CASCADE")
                cur.execute(
                    f'CREATE TABLE emoji_{feeling} ('
                    f'id SERIAL PRIMARY KEY,'
                    f'word varchar(255) UNIQUE NOT NULL, '
                    f'w_count integer)'
                )
                for key, value in w_list.items():
                    key = key.replace("\'", "")
                    cur.execute(
                        f'INSERT INTO emoji_{feeling}(word, w_count)'
                        f'VALUES(\'{key}\', 1)'
                        f'ON  CONFLICT (word) '
                        f'DO UPDATE  SET w_count = emoji_{feeling}.w_count + 1'
                    )
                # cur.execute(f"SELECT * FROM tweet_{feeling}")
                # print(cur.fetchall())
        cur.close()
        self.conn.commit()

    def create_hashtag_table(self):
        cur = self.conn.cursor()
        if len(self.hashtag) > 0:
            for feeling, w_list in self.hashtag.items():
                cur.execute(f"DROP TABLE IF EXISTS hashtag_{feeling} CASCADE")
                cur.execute(
                    f'CREATE TABLE hashtag_{feeling} ('
                    f'id SERIAL PRIMARY KEY,'
                    f'word varchar(255) UNIQUE NOT NULL, '
                    f'w_count integer)'
                )
                for key, value in w_list.items():
                    key = key.replace("\'", "")
                    cur.execute(
                        f'INSERT INTO hashtag_{feeling}(word, w_count)'
                        f'VALUES(\'{key}\', 1)'
                        f' ON  CONFLICT (word) '
                        f'DO UPDATE  SET w_count = hashtag_{feeling}.w_count + 1'
                    )
                # cur.execute(f"SELECT * FROM tweet_{feeling}")
                # print(cur.fetchall())
        cur.close()
        self.conn.commit()


import psycopg2

class PGConnection:

    def __init__(self):
        self.conn = None
        self.create_pg_connection()

    def create_pg_connection(self):
        try:
            self.conn = psycopg2.connect('dbname=maadbsql user=maadbsql host=localhost port=5433')
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error while postgress connection: ", error)



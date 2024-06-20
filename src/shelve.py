import os

import psycopg2

class PgShelve:
    def __init__(self):
        DATABASE_URL = os.environ['DATABASE_URL']
        conn = psycopg2.connect(DATABASE_URL, sslmode='require')
        self.conn = conn

        self._checkTable()

        self.dict = self.fetch()

    def _checkTable(self):
        create=f"CREATE TABLE IF NOT EXISTS PAGES (URL VARCHAR(255) NOT NULL, STATE VARCHAR(255) NOT NULL)"
        cur = self.conn.cursor()
        cur.execute(create)

    def fetch(self):
        dict = {}
        cur = self.conn.cursor()
        cur.execute("SELECT URL,STATE FROM PAGES")
        for row in cur.fetchall():
            dict[row[0]] = str(row[1])
        cur.close()
        return dict

    def keyExists(self, key, cur):
        cur.execute("SELECT COUNT(*) FROM PAGES WHERE URL = %s", (key,))
        return cur.fetchone()[0] == 1

    def __setitem__(self, key, item):
        self.dict[key] = item
        cur = self.conn.cursor()
        val = str(item)
        if self.keyExists(key, cur):
            cur.execute("UPDATE PAGES SET STATE = %s WHERE URL = %s", (val, key,))
        else:
            cur.execute("INSERT INTO PAGES (URL, STATE) VALUES (%s, %s)", (key, val,))
        self.conn.commit()
        cur.close()

    def __getitem__(self, key):
        return self.dict[key]

    def keys(self):
        return self.dict.keys()

    def sync(self):
        pass
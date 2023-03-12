import sqlite3
from sqlite3 import Error

class DBmarket:
    def __init__(self, db_file):
        self.conn = None
        self.sql_create_projects_table = """ CREATE TABLE IF NOT EXISTS markets (
                                            year  integer NOT NULL,
                                            month integer NOT NULL,
                                            day   integer NOT NULL,
                                            code  text NOT NULL,
                                            longPositions int,
                                            shortPositions int,
                                            name text
                                        ); """
        try:
            self.conn = sqlite3.connect(db_file)
            print(sqlite3.version)
            self.create_table
        except Error as e:
            print(e)

    def create_table(self):
        try:
            c = conn.cursor()
            c.execute(create_table_sql)
        except Error as e:
            print(e)

    def add_records(self, records):
        sql = ''' INSERT INTO markets(year, month, day, code, longPositions, shortPositions, name)
                  VALUES(?,?,?,?,?,?,?) '''
        cur = None
        for r in records:
            if self.select_record(r) == 0:
                cur = self.conn.cursor()
                task = (r['year'], r['month'], r['day'], r['code'], r['longPositions'], r['shortPositions'], r['symbol'])
                cur.execute(sql, task)
                self.conn.commit()
        if cur is not None:
            return cur.lastrowid
        else:
            return None

    def select_record(self, record):
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM markets WHERE year=? and month=? and day=? and code=?", (record['year'], record['month'], record['day'], record['code'],))
        rows = cur.fetchall()
        i = 0
        for row in rows:
            i+=1
        return i
import sqlite3
from datetime import datetime
import collections


def flatten(l):
    for el in l:
        if isinstance(el, collections.abc.Iterable) and not isinstance(el, (str, bytes)):
            yield from flatten(el)
        else:
            yield el


class DB:
    def __init__(self, date, BOOK_DEPTH):
        self.date = date
        self.BOOK_DEPTH = BOOK_DEPTH
        self.connect(self.date)

    def connect(self, date):
        self.close()
        self.conn = sqlite3.connect(f'dblogs/{date}.db')
        self.conn.row_factory = sqlite3.Row
        self.c = self.conn.cursor()
        str = ''
        for i in range(1, self.BOOK_DEPTH+1):
            if(str != ''):
                str += ', '
            str += f'ask{i} REAL, asksize{i} REAL'

        for i in range(1, self.BOOK_DEPTH+1):
            if(str != ''):
                str += ', '
            str += f'bid{i} REAL, bidsize{i} REAL'
        self.c.execute(
            f'CREATE TABLE IF NOT EXISTS btcjpy(exchange TEXT, unixtime INT, {str});')
        self.c.execute(
            f'create index if not exists timeindex on btcjpy(exchange, unixtime);')
        self.sql = f'INSERT INTO btcjpy VALUES({",".join(["?"] * (2 + self.BOOK_DEPTH * 4))})'

    def save(self, exchange, timestamp, asks, bids):
        cur_date = datetime.fromtimestamp(timestamp).strftime('%Y%m%d')
        if cur_date != self.date:
            self.connect(cur_date)
        data = list(flatten([exchange, timestamp, asks, bids]))
        self.c.execute(self.sql, data)
        self.conn.commit()

    def get(self, exchange, start, end):
        sql = f'SELECT * FROM btcjpy where exchange = "{exchange}" AND unixtime >= {start} AND unixtime < {end} order by unixtime asc'

        resp = []
        for row in self.c.execute(sql):
            asks = []
            bids = []
            for i in range(1, self.BOOK_DEPTH):
                asks.append([row[f'ask{i}'] / 1000000, row[f'asksize{i}']])
                bids.append([row[f'bid{i}'] / 1000000, row[f'bidsize{i}']])
            resp.append({
                'timestamp': row['unixtime'],
                'asks': asks,
                'bids': bids
            })
        return resp

    def close(self):
        if hasattr(self, 'conn'):
            self.conn.close()

#!/usr/bin/python
import zlib
import boto
import getopt, argparse
import json
import sys 
import MySQLdb
from seldon import fileutil as fu
import logging

logger = logging.getLogger(__name__)

class MostPopularUploadMysql(object):
    """
    Upload results of most popular counts to mysql database for serving
    """
    def __init__(self,client,db_host,db_user,db_pass):
        self.db = MySQLdb.connect(
            host=db_host,
            user=db_user,
            passwd=db_pass,
            db=client
        )

        self.db.set_character_set('utf8')
        self.count = 0
        self.rows = 0
        self.inserts = []
        self.DB_BATCH_SIZE = 5000
        self.SQL_INSERT = "insert into items_recent_popularity_new (item_id,score) values (%(item)s,%(count)s)"

    def truncate_table(self):
        logger.info("truncate table")
        dbc = self.db.cursor()
        dbc.execute('truncate items_recent_popularity_new')
        dbc.close()

    def rename_table(self):
        dbc = self.db.cursor()
        dbc.execute('rename table items_recent_popularity to items_recent_popularity_old,items_recent_popularity_new to items_recent_popularity,items_recent_popularity_old to items_recent_popularity_new')
        dbc.close()

    def add_popular_assets(self):
        logger.info("adding assets to recent popularity")
        dbc = self.db.cursor()
        dbc.execute('insert ignore into items_recent_popularity select * from items_recent_popularity_assets')
        dbc.close()

    def reallyDoInserts(self,params):
        cur = self.db.cursor()
        cur.executemany(self.SQL_INSERT, params)
        cur.close()

    def upload(self,line):
        line = line.rstrip()
        j = json.loads(line)
        item = int(j['item'])
        count = float(j['count'])
        self.inserts.append({'item': item, 'count': count})
        if len(self.inserts) > self.DB_BATCH_SIZE:
            self.count += 1
            self.rows += self.DB_BATCH_SIZE
            logger.info("Running batch %d rows inserted %d",self.count,self.rows)
            self.reallyDoInserts(self.inserts)
            self.inserts = []

    def stream_and_upload(self,folder):
        self.truncate_table()
        futl = fu.FileUtil()
        futl.stream(folder,self.upload)
        if len(self.inserts) > 0:
            logger.info("Running final batch with rows inserted %d",self.rows)
            self.reallyDoInserts(self.inserts)
        self.rename_table()
        self.add_popular_assets()
        self.db.commit()



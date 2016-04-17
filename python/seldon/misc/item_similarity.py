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

class ItemSimilarityUploadMysql(object):
    """
    Upload results of item similarity training to mysql database
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

    def truncate_table(self):
        logger.info("truncate table")
        dbc = self.db.cursor()
        dbc.execute('truncate item_similarity_new')
        dbc.close()

    def rename_table(self):
        logger.info("renaming table")
        dbc = self.db.cursor()
        dbc.execute('rename table item_similarity to item_similarity_old,item_similarity_new to item_similarity,item_similarity_old to item_similarity_new')
        dbc.close()

    def reallyDoInserts(self,params):
        cur = self.db.cursor()
        cur.executemany("insert into item_similarity_new values (%(item1)s,%(item2)s,%(sim)s)", params)
        cur.close()

    def upload(self,line):
        line = line.rstrip()
        j = json.loads(line)
        item1 = int(j['item1'])
        item2 = int(j['item2'])
        sim = float(j['sim'])
        if item1>0 and item2>0:
            self.inserts.append({'item1': item1, 'item2': item2, 'sim': sim})
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
        self.db.commit()



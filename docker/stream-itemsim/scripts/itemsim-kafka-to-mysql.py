import os
import sys, getopt, argparse
import logging
import json
from kafka import KafkaConsumer
import MySQLdb

INSERT_SQL = "insert into item_similarity_new (item_id,item_id2,score) values (%(item_id)s,%(item_id2)s,%(score)s)"
ADD_PREV_RESULTS_SQL = "insert ignore into item_similarity_new select item_id,item_id2,score-0.01 from item_similarity"
REMOVE_OLD_RESULTS_SQL = "delete from item_similarity_new where score<0"
RENAME_TABLE_SQL = "rename table item_similarity to item_similarity_tmp,item_similarity_new to item_similarity,item_similarity_tmp to item_similarity_new"
BATCH_SIZE = 3000

class KafkaToMysql(object):

    def __init__(self,db_settings,client_name):
        self.db = MySQLdb.connect(
            host=db_settings['host'],
            user=db_settings['user'],
            passwd=db_settings['password'],
            db=client_name)

    def begin_insert(self):
        self.dbc = self.db.cursor()
        self.dbc.execute("truncate item_similarity_new")
        self.inserts = []
        self.batch = 0

    def add_similarity(self,item1,item2,score,time):
        self.inserts.append({"item_id":item1,"item_id2":item2,"score":score})
        if len(self.inserts) > BATCH_SIZE:
            self.batch += 1
            self.run_inserts(self.inserts,self.batch)
            self.inserts = []

    def end_insert(self):
        if len(self.inserts) > 0:
            self.batch += 1
            self.run_inserts(self.inserts,self.batch)
        self.dbc.execute(ADD_PREV_RESULTS_SQL)
        self.dbc.execute(REMOVE_OLD_RESULTS_SQL)
        self.dbc.execute(RENAME_TABLE_SQL)
        self.db.commit()
        self.dbc.close()

    def run_inserts(self,inserts,batch):
        logger.info("inserting batch %d into the db",batch)
        self.dbc.executemany(INSERT_SQL, inserts)

    def run(self,kafka_server,topic):
        state = "WAITING"
        consumer = KafkaConsumer(topic,bootstrap_servers=kafka_server)
        for msg in consumer:
            logger.info(msg)
            (time,item1,item2,score) = msg.value.split(",")
            time = int(time)
            if time == 0:
                if state == "WAITING":
                    self.begin_insert()
                    state = "INSERTING"
                elif state == "INSERTING":
                    self.end_insert()
                    state = "WAITING"
                else:
                    logger.info("bad state! found start/end marking but state is %s",state)
            elif state == "INSERTING":
                item1 = int(item1)
                item2 = int(item2)
                score = float(score)
                self.add_similarity(item1,item2,score,time)
            else:
                logger.info("bad state! found something to insert but state is %s",state)
            logger.info("STATE is now %s",state)

if __name__ == '__main__':
    import logging
    logger = logging.getLogger()
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(name)s : %(message)s', level=logging.DEBUG)
    logger.setLevel(logging.INFO)

    parser = argparse.ArgumentParser(prog='create_replay')
    parser.add_argument('--kafka', help='kafka server', default="localhost:9092")
    parser.add_argument('--topic', help='topic', required=True)
    parser.add_argument('--mysql-host', help='mysql host', required=True)
    parser.add_argument('--mysql-user', help='mysql user', required=True)
    parser.add_argument('--mysql-password', help='mysql password', required=True)
    parser.add_argument('--client', help='seldon client', required=True)

    args = parser.parse_args()
    opts = vars(args)

    db_settings = {}
    db_settings['host'] = args.mysql_host
    db_settings['user'] = args.mysql_user
    db_settings['password'] = args.mysql_password

    k = KafkaToMysql(db_settings,args.client)
    k.run(args.kafka,args.topic)


import time
import datetime
import sys
import getopt, argparse
from collections import defaultdict
import json
import MySQLdb
import unicodecsv
import pprint

USER_INSERT = "insert into users (client_user_id, username, first_op, last_op,type,num_op, active) values (%(id)s, %(name)s, now(), now(), 1,1,1)"

def pp(o):
    p = pprint.PrettyPrinter(indent=4)
    p.pprint(o)

def validateCSV(csv_file):
	with open(csv_file) as csvFile:
		reader = unicodecsv.DictReader(csvFile,encoding='utf-8')
		line = reader.next()
		for field_name in line:
			if not field_name == 'id' and not field_name == 'username':
				print 'only id or username fields allowed'
				exit(1)

def doUserInserts(csv_file, db):
	with open(csv_file) as csvFile:
		reader = unicodecsv.DictReader(csvFile,encoding='utf-8')
		inserts = []
		insertNum = 0
		for line in reader:
			client_id = line['id']
			name = ''
			if 'name' in line:
				name = line['name']
			inserts.append({'name':name,'id':client_id})
			if len(inserts) > 1000:
				insertNum+=1
				reallyDoInserts(USER_INSERT, inserts, insertNum, db)
				inserts = []

		insertNum+=1
		reallyDoInserts(USER_INSERT, inserts, insertNum, db)
		db.commit()
		print 'finished user inserts'

def reallyDoInserts(insertStatement, params, num, db):
	cur = db.cursor()
	print "inserting user batch", num,'into the db'
	cur.executemany(insertStatement, params)


def cleanUpDb(db):
	dbc = db.cursor()
	dbc.execute('truncate table users')

def import_users(client_name, db_settings, data_file_fpath):
    db = MySQLdb.connect(
            host=db_settings['host'],
            user=db_settings['user'],
            passwd=db_settings['password'],
            db=client_name
    )

    db.set_character_set('utf8')
    dbc = db.cursor()
    dbc.execute('SET NAMES utf8;')
    dbc.execute('SET CHARACTER SET utf8;')
    dbc.execute('SET character_set_connection=utf8;')
    dbc.execute("SET GLOBAL max_allowed_packet=1073741824")
    try:
            validateCSV(data_file_fpath)
            doUserInserts(data_file_fpath, db)
    except:
            print 'Unexpected error ...', sys.exc_info()[0]
            print 'Clearing DB of users'
            try:
                    cleanUpDb(db)
            except:
                    print 'couldn\'t clean up db'
            raise
    print "Successfully ran all inserts"


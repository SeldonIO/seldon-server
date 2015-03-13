#!/usr/bin/env python
import time
import datetime
import sys
import getopt, argparse
from collections import defaultdict
import json
import MySQLdb
import unicodecsv
USER_INSERT = "insert into users (client_user_id, username, first_op, last_op,type,num_op, active) values (%(id)s, %(name)s, now(), now(), 1,1,1)"


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

parser = argparse.ArgumentParser(prog="add_users.py")
parser.add_argument('-users', help='user csv file', required=True)
parser.add_argument('-db-host', help='database host', required=False, default="localhost")
parser.add_argument('-db-user', help='database username', required=False, default="root")
parser.add_argument('-db-pass', help='database password', required=False, default="root")
parser.add_argument('-client', help='client/database name', required=False, default="testclient")

opts = vars(parser.parse_args())

db = MySQLdb.connect(user=opts['db_user'],db=opts['client'],passwd=opts['db_pass'], host=opts['db_host'])
db.set_character_set('utf8')
dbc = db.cursor()
dbc.execute('SET NAMES utf8;')
dbc.execute('SET CHARACTER SET utf8;')
dbc.execute('SET character_set_connection=utf8;')
dbc.execute("SET GLOBAL max_allowed_packet=1073741824")
try:
	validateCSV(opts['users'])
	doUserInserts(opts['users'], db)
except:
	print 'Unexpected error ...', sys.exc_info()[0]
	print 'Clearing DB of users'
	try:
		cleanUpDb(db)
	except:
		print 'couldn\'t clean up db'
	raise
print "Successfully ran all inserts"
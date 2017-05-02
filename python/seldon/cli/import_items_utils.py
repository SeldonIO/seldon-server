import time
import datetime
import sys
import getopt, argparse
from collections import defaultdict
import json
import MySQLdb
import unicodecsv
import pprint

ITEM_MAP_VARCHAR_INSERT = "insert into item_map_varchar (item_id, attr_id, value) values ((select item_id from items where client_item_id = %(id)s),(select attr_id from item_attr where name = %(attr_name)s), %(value)s )"
ITEM_MAP_DOUBLE_INSERT = "insert into item_map_double (item_id, attr_id, value) values ((select item_id from items where client_item_id = %(id)s),(select attr_id from item_attr where name = %(attr_name)s), %(value)s )"
ITEM_MAP_DATETIME_INSERT = "insert into item_map_datetime (item_id, attr_id, value) values ((select item_id from items where client_item_id = %(id)s),(select attr_id from item_attr where name = %(attr_name)s), %(value)s )"
ITEM_MAP_ENUM_INSERT = "insert into item_map_enum (item_id, attr_id, value_id) values ((select item_id from items where client_item_id = %(id)s),(select attr_id from item_attr where name = %(attr_name)s), (select e.value_id from ITEM_ATTR_ENUM e, item_attr a where a.name = %(attr_name)s and e.value_name = %(value)s and a.attr_id = e.attr_id) )"
ITEM_MAP_TEXT_INSERT = "insert into item_map_text (item_id, attr_id, value) values ((select item_id from items where client_item_id = %(id)s),(select attr_id from item_attr where name = %(attr_name)s), %(value)s )"
ITEM_MAP_INT_INSERT = "insert into item_map_int (item_id, attr_id, value) values ((select item_id from items where client_item_id = %(id)s),(select attr_id from item_attr where name = %(attr_name)s), %(value)s )"
ITEM_MAP_BOOLEAN_INSERT = "insert into item_map_boolean (item_id, attr_id, value) values ((select item_id from items where client_item_id = %(id)s),(select attr_id from item_attr where name = %(attr_name)s), %(value)s )"

ITEM_INSERT = "INSERT INTO ITEMS (name, first_op, last_op, client_item_id, type) VALUES (%(name)s, NOW(), NOW(), %(id)s, 1)"
ITEM_INSERT_NO_AUTO_INCREMENT = "INSERT INTO ITEMS (item_id, name, first_op, last_op, client_item_id, type) VALUES (%(item_id)s, %(name)s, NOW(), NOW(), %(id)s, 1)"
DB_BATCH_SIZE = 1000
attr_insert_map = {
	'ENUM': ITEM_MAP_ENUM_INSERT,
	'BOOLEAN': ITEM_MAP_BOOLEAN_INSERT,
	'VARCHAR': ITEM_MAP_VARCHAR_INSERT,
	'TEXT': ITEM_MAP_TEXT_INSERT,
	'DATETIME': ITEM_MAP_DATETIME_INSERT,
	'INT': ITEM_MAP_INT_INSERT,
	'DOUBLE': ITEM_MAP_DOUBLE_INSERT
}


available_attrs = dict()
available_enums = dict()

def pp(o):
    p = pprint.PrettyPrinter(indent=4)
    p.pprint(o)

def retrieveDbAttrs(db):
	cur = db.cursor()
	cur.execute("SELECT ATTR_ID, NAME, TYPE FROM ITEM_ATTR")
	rows = cur.fetchall()
	attrs = dict()
	for row in rows:
		attrs[row[1]]= (row[0], row[2])

	return attrs

def retrieveDbEnums(db):
	cur = db.cursor()
	# enum structure:
	#    attr_id1:
	#				value_name1 : value_id1
	#				value_name2 :value_id2
	cur.execute("SELECT ATTR_ID, VALUE_NAME, VALUE_ID FROM ITEM_ATTR_ENUM")
	rows = cur.fetchall()
	enums = defaultdict(dict)
	for row in rows:
		enums[row[0]][row[1]] = row[2]

	return enums

def validateCSVAgainstDb(csv_file, db):
	global available_attrs, available_enums
	failed = False
	attrs = retrieveDbAttrs(db)
	available_attrs = attrs
	enums = retrieveDbEnums(db)
	available_enums = enums
	with open(csv_file) as csvFile:
		reader = unicodecsv.DictReader(csvFile,encoding='utf-8')
		noOfFields = 0
		for index, line in enumerate(reader, start=1):
			if index is 1:
				noOfFields = len(line)
				if not validateFieldsAgainstDbFields(set(line), attrs, db):
					exit(1)
			validateLine(index,line, noOfFields, attrs, enums)
			if len(line) != noOfFields:
				failLine(index, line)
				failed = True


	if failed:
		exit(1)

def validateLine(index,line, noOfFields, attrs, enums):
	if len(line) != noOfFields:
		failLine(index, line)
		failed = True
	else:
		for word in line:
			if str(word) == 'id':
				continue
			if str(word) == 'name':
				continue
			value = line[word]
			if str(attrs[word][1]) == 'ENUM':
				if value not in enums[attrs[word][0]]:
					print 'couldn\'t find enum value', value
					exit(1)


def validateFieldsAgainstDbFields(fields,attrs,  db):
	failed = False
	for field in fields:
		if field not in attrs and field != 'id' and field != 'name':
			failed = True
			print 'Field \'',field,'\'not an attribute in the DB'

	return not failed

def doItemInserts(csv_file, db):
	insertNum = 0
	with open(csv_file) as csvFile:
		reader = unicodecsv.DictReader(csvFile,encoding='utf-8')
		inserts = []
		for line in reader:
			client_id = line['id']
			name = ''
			if 'name' in line:
				name = line['name']
                        inserts.append({'name':name,'id':client_id, 'item_id':client_id})
                        insertNum+=1
                        if insertNum >= DB_BATCH_SIZE:
                            cur = db.cursor()
                            print "inserting batch items into the db"
                            cur.executemany(ITEM_INSERT_NO_AUTO_INCREMENT, inserts)
                            insertNum = 0
                            inserts = []
                if insertNum > 0:
                    cur = db.cursor()
                    print "inserting final batch items into the db"
                    cur.executemany(ITEM_INSERT_NO_AUTO_INCREMENT, inserts)
		db.commit()
		print 'finished item inserts'

def doAttrInserts(csv_file, db):
	inserts = defaultdict(list)
	insertNum = 0
	with open(csv_file) as csvFile:
		reader = unicodecsv.DictReader(csvFile,encoding='utf-8')
		for line in reader:
			for field_name in line:
				if field_name == 'id' or field_name== 'name':
					continue
				attr_type = available_attrs[str(field_name)][1]
				inserts[attr_type].append({'attr_name': field_name, 'value': line[field_name], 'id': line['id']})
				if len(inserts[attr_type]) > DB_BATCH_SIZE:
					insertNum+=1
					reallyDoInserts(inserts[attr_type], attr_insert_map[attr_type], insertNum, db)
					del inserts[attr_type]
	for index, insert_label in enumerate(inserts, start=1):
		insertNum+=1
		reallyDoInserts(inserts[insert_label], attr_insert_map[insert_label], insertNum, db)
	db.commit()
	print 'finished attribute inserts'

def reallyDoInserts(params, insertStatement, insertNum, db):
		cur = db.cursor()
		print "inserting attribute batch", insertNum,'into the db'
		cur.executemany(insertStatement, params)

def failLine(lineNum, line):
	print "line",lineNum,"failed as it only had",len(line),"fields"

def cleanUpDb(db):
	dbc = db.cursor()
	dbc.execute('truncate table items')
	dbc.execute('truncate table item_map_varchar')
	dbc.execute('truncate table item_map_double')
	dbc.execute('truncate table item_map_datetime')
	dbc.execute('truncate table item_map_int')
	dbc.execute('truncate table item_map_boolean')
	dbc.execute('truncate table item_map_enum')
	dbc.execute('truncate table item_map_text')
	db.commit()

def import_items(client_name, db_settings, data_file_fpath):
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
    #dbc.execute("SET GLOBAL max_allowed_packet=1073741824")
    try:
            validateCSVAgainstDb(data_file_fpath, db)
            doItemInserts(data_file_fpath, db)
            doAttrInserts(data_file_fpath,db)
    except:
            print 'Unexpected error ...', sys.exc_info()[0]
            print 'Clearing DB of items and attributes'
            try:
                    cleanUpDb(db)
            except:
                    print 'couldn\'t clean up db'
            raise
    print "Successfully ran all inserts"


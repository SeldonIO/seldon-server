import json
import MySQLdb
import getopt, argparse

import pprint

def pp(o):
    p = pprint.PrettyPrinter(indent=4)
    p.pprint(o)

valid_value_types = set(['double', 'string', 'date', 'text', 'int','boolean'])
value_types_to_db_map = dict(double='DOUBLE', string='VARCHAR', date='DATETIME', int='INT', boolean='BOOLEAN',
	text='TEXT', enum='ENUM')

def hasAttr(attrs,name):
    for attr in attrs:
        if attr["name"] == name:
            return True
    return False

def addAttrsToDb(db, attrs, item_type):
    if not hasAttr(attrs,"content_type"):
        attrs.append({"name":"content_type", "value_type":["article"]})
    for attr in attrs:
        print "adding item type",item_type,"attribute ",attr['name']
        attrValType = attr['value_type']
        if type(attrValType) is list:
            attrValType = 'enum'
        cur = db.cursor()
        cur.execute("INSERT INTO ITEM_ATTR (name, type, item_type) "
                    + " VALUES (%s, %s, %s)", (attr['name'], value_types_to_db_map[attrValType], item_type))
        if attrValType is 'enum':
            for index,enum in enumerate(attr['value_type'], start=1):
                cur = db.cursor()
                cur.execute("SELECT attr_id FROM ITEM_ATTR WHERE NAME = %s and ITEM_TYPE = %s", (attr['name'],item_type))
                rows = cur.fetchall()
                attrId = rows[0][0]
                cur = db.cursor()
                cur.execute("INSERT INTO ITEM_ATTR_ENUM (attr_id, value_id, value_name) VALUES (%s, %s, %s)",(attrId, index, enum))
    cur = db.cursor()
    cur.execute("SELECT e.attr_id, e.value_id, e.value_name FROM ITEM_ATTR_ENUM e join item_attr a on (a.attr_id=e.attr_id and a.item_type=%s)",(item_type,))
    rows = cur.fetchall()
    for row in rows:
            enum_attr_id = row[0]
            enum_value_id = row[1]
            enum_value_name = row[2]
            cur = db.cursor()
            cur.execute("INSERT INTO DIMENSION (item_type, attr_id, value_id) VALUES"
                    + " (%s, %s, %s)", (item_type, enum_attr_id, enum_value_id))

def doDbChecks(db):
	cur = db.cursor()
	cur.execute("SELECT COUNT(*) FROM ITEM_TYPE")
	rows = cur.fetchall()
	if rows[0][0] != 0:
		print "ITEM_TYPE table was not empty, it had", rows[0][0], 'rows'
		doExitBecauseDbNotEmpty()
	cur = db.cursor()
	cur.execute("SELECT COUNT(*) FROM ITEM_ATTR")
	rows = cur.fetchall()
	if rows[0][0] != 0:
		print "ITEM_ATTR table was not empty, it had", rows[0][0], 'rows'
		doExitBecauseDbNotEmpty()
	cur = db.cursor()
	cur.execute("SELECT COUNT(*) FROM ITEM_ATTR_ENUM")
	rows = cur.fetchall()
	if rows[0][0] !=0:
		print "ITEM_ATTR_ENUM table was not empty, it had", rows[0][0], 'rows'
		doExitBecauseDbNotEmpty()
	cur = db.cursor()
	cur.execute("SELECT COUNT(*) FROM DIMENSION")
	rows = cur.fetchall()
	if rows[0][0] !=0:
		print "DIMENSION table was not empty, it had", rows[0][0], 'rows'
		doExitBecauseDbNotEmpty()


def doExitBecauseDbNotEmpty():
	print "To run this script, the relevant DB tables must be empty. Please rerun this script with the -clean option to delete these entries."
	exit(1)

def addToDb(db, types):
	with db:
		doDbChecks(db)
		for theType in types:
			cur= db.cursor()
			cur.execute("INSERT INTO ITEM_TYPE (type_id, name)"+
				" values (%s, %s)",(theType['type_id'],theType['type_name']))
			addAttrsToDb(db, theType['type_attrs'], theType['type_id'])


def validateValueType(valType):
    theType = type(valType)
    if theType is list:
        for enum in valType:
            theEnumType = type(enum)
            if theEnumType is not unicode and theEnumType is not str:
                print "enum objects must be strings:", theEnumType
                exit(1)
    elif theType is unicode:
        if valType not in valid_value_types:
            print "the value type must be one of 'double', 'string', 'date' or an object"
            exit(1)
    else:
        print "the type of the field value_type must be a string or a list where as it was",theType
        exit(1)

def validateAttr(theAttr):
    if 'name' not in theAttr or 'value_type' not in theAttr:
        print "couldn't find one of (name, value_type) for attr "
        pp(theAttr)
        exit(1)
    else:
        validateValueType(theAttr['value_type']);

def validateType(theType):
    if 'type_id' not in theType or 'type_name' not in theType or 'type_attrs' not in theType:
        print "couldn't find one of (type_id, type_name, type_attrs) for object"
        pp(theType)
        exit(1)
    for theAttr in theType['type_attrs']:
        validateAttr(theAttr)

def validateNumbering(types):
    ids = set()
    for theType in types:
        if isinstance(theType['type_id'], int):
            if theType['type_id'] in ids:
                print "found a repeated type_id", theType['type_id']
                exit(1)
            else:
                ids.add(theType['type_id'])
        else:
            print "type_id s must be integers but one was","\"",theType['type_id'],"\""
            exit(1)

def outputDimensionsToFile(file, db):

	with db:
		cur = db.cursor()
		cur.execute("SELECT d.dim_id, e.value_name from DIMENSION d, ITEM_ATTR_ENUM e where d.attr_id = e.attr_id and d.value_id = e.value_id and e.value_name != \'article\'")
		rows = cur.fetchall()
		json.dump(rows, file)

def readTypes(types):
    for theType in types:
        validateType(theType)
    validateNumbering(types)
    return types

def clearUp(db):
	with db:
		cur = db.cursor()
		cur.execute("TRUNCATE TABLE ITEMS")
		cur.execute("TRUNCATE TABLE DIMENSION")
		cur.execute("TRUNCATE TABLE ITEM_ATTR_ENUM")
		cur.execute("TRUNCATE TABLE ITEM_ATTR")
		cur.execute("TRUNCATE TABLE ITEM_TYPE")
		#cur.execute('truncate table users')
		cur.execute('truncate table items')
		cur.execute('truncate table item_map_varchar')
		cur.execute('truncate table item_map_double')
		cur.execute('truncate table item_map_datetime')
		cur.execute('truncate table item_map_int')
		cur.execute('truncate table item_map_boolean')
		cur.execute('truncate table item_map_enum')
		cur.execute('truncate table item_map_text')

def create_schema(client_name, dbSettings, scheme_file_path, clean=False):

    if clean != True:
        json_data=open(scheme_file_path)
        data = json.load(json_data)
        if 'types' not in data:
            print "couldn't find types object in json"
            return
        else:
            types = readTypes(data['types'])

    db = MySQLdb.connect(user=dbSettings["user"],db=client_name,passwd=dbSettings["password"], host=dbSettings["host"])
    if clean == True:
        clearUp(db)
        print "Finished cleaning attributes successfully"
    else:
        addToDb(db, types)
        f = open('dimensions.json','w')
        outputDimensionsToFile(f,db)

        print 'Finished applying attributes successfully'

        json_data.close()


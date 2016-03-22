import time
import datetime
import sys
import getopt, argparse
from collections import defaultdict
import json
import MySQLdb
import unicodecsv
import pprint

def pp(o):
    p = pprint.PrettyPrinter(indent=4)
    p.pprint(o)

def getItemId(db,cache,client_item_id):
    if client_item_id in cache:
        return cache[client_item_id]
    else:
        cursor = db.cursor()
        cursor.execute("""select item_id, client_item_id from items""")
        rows = cursor.fetchall()
        for row in rows:
            itemId = long(row[0])
            client_item_id_from_db = row[1]
            cache[client_item_id_from_db] = itemId
        cursor.close()
        return cache[client_item_id]

def getUserId(db,cache,client_user_id):
    if client_user_id in cache:
        return cache[client_user_id]
    else:
        cursor = db.cursor()
        cursor.execute("""select user_id,client_user_id from users""")
        rows = cursor.fetchall()
        for row in rows:
            userId = long(row[0])
            client_user_id_from_db = row[1]
            cache[client_user_id_from_db] = userId

        cursor.close()
        return cache[client_user_id]

def import_actions(client_name, db_settings, data_file_fpath, out_file_fpath):

    db = MySQLdb.connect(
            host=db_settings['host'],
            user=db_settings['user'],
            passwd=db_settings['password'],
            db=client_name
    )

    userCache = {}
    itemCache = {}
    count = 0
    with open(data_file_fpath) as csvfile, open(out_file_fpath,'w') as outfile:
        reader = unicodecsv.DictReader(csvfile,encoding='utf-8')
        for f in reader:
            item = getItemId(db,itemCache,f["item_id"])
            user = getUserId(db,userCache,f["user_id"])
            action_type = 1
            action = {}
            action["userid"] = int(user)
            action["client_userid"] = f["item_id"]
            action["itemid"] = int(item)
            action["client_itemid"] = f["user_id"]
            action["value"] = float(f["value"])
            utc = datetime.datetime.fromtimestamp(int(f["time"])).strftime('%Y-%m-%dT%H:%M:%SZ')
            action["timestamp_utc"] = utc
            action["rectag"] = "default"
            action["type"] = action_type
            action["client"] = client_name
            s = json.dumps(action,sort_keys=True)
            outfile.write(s+"\n")
            count += 1
            if count % 50000 == 0:
                print "Processed "+str(count)+" actions"


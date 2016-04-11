import pprint
import argparse
import os
import sys
import re
import json
import MySQLdb


KEYS_SQL_CLIENT = "select short_name,consumer_key,consumer_secret,scope from api.consumer where short_name=%(client_name)s"
KEYS_SQL_CLIENT_SCOPE = "select short_name,consumer_key,consumer_secret,scope from api.consumer where short_name=%(client_name)s and scope=%(scope)s"
KEYS_SQL_SCOPE = "select short_name,consumer_key,consumer_secret,scope from api.consumer where scope=%(scope)s"
KEYS_SQL = "select short_name,consumer_key,consumer_secret,scope from api.consumer"

def get_keys(dbSettings,client_name,scope):
    db = MySQLdb.connect(host=dbSettings["host"],
                         user=dbSettings["user"],
                         passwd=dbSettings["password"])
    cur = db.cursor()
    if client_name is None and scope is None:
        sql = KEYS_SQL
    elif client_name is None and not scope is None:
        sql = KEYS_SQL_SCOPE
    elif scope is None and not client_name is None:
        sql = KEYS_SQL_CLIENT
    else:
        sql = KEYS_SQL_CLIENT_SCOPE
    cur.execute(sql,{"client_name":client_name,"scope":scope})
    rows = cur.fetchall()
    res = []
    for row in rows:
        res.append({"db":dbSettings["name"],"client":row[0],"key":row[1],"secret":row[2],"scope":row[3]})
    cur.close()    
    return res

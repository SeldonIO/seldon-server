#!/usr/bin/python
import getopt, argparse
import json
import sys 

batchSize = 5000
numInserts = 0
sqlInsertPrefix = "insert into item_similarity_new values "
print "truncate item_similarity_new;"
sql = sqlInsertPrefix
for line in sys.stdin:
    line = line.rstrip()
    j = json.loads(line)
    item1 = j['item1']
    item2 = j['item2']
    sim = j['sim']
    if numInserts > 0:
        sql = sql + ","
    sql = sql +  " (%s,%s,%s)" % (item1,item2,sim)
    numInserts += 1
    if numInserts >= batchSize:
        sql = sql + ";"
        print sql
        numInserts = 0;
        sql = sqlInsertPrefix

if numInserts > 0:
    sql = sql + ";"
    print sql;

print "rename table item_similarity to item_similarity_old,item_similarity_new to item_similarity,item_similarity_old to item_similarity_new;"





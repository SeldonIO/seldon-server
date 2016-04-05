import json
import codecs 
import os
import unicodecsv
from collections import OrderedDict
import os

#data_size = "justTen"
data_size = "full"
outfile = os.path.abspath("reuters-21578.csv")

row_count = 0
ordered_fieldnames = OrderedDict([('id',None),('title',None),('body',None)])
with open(outfile,'wt') as f:
    dw = unicodecsv.DictWriter(f, delimiter=',', fieldnames=ordered_fieldnames, encoding='utf-8')
    dw.writeheader()
    
    for filename in os.listdir("reuters-21578-json/data/"+data_size):
        f = open("reuters-21578-json/data/"+data_size+"/"+filename)
        js = json.load(f)
        for j in js:
            if 'topics' in j and 'body' in j:
                r = {}
                r["id"] = j['id']
                r["title"] = j['title']
                r["body"] = j['body']
                dw.writerow(r)
                row_count += 1

print "finished writing csv data".format(**locals())
print "rows: {row_count}".format(**locals())
print "file: {outfile}".format(**locals())


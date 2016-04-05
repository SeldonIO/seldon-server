import json
import codecs 
import os

docs = []
for filename in os.listdir("reuters-21578-json/data/full"):
    f = open("reuters-21578-json/data/full/"+filename)
    js = json.load(f)
    for j in js:
        if 'topics' in j and 'body' in j:
            d = {}
            d["id"] = j['id']
            d["text"] = j['body'].replace("\n","")
            d["title"] = j['title']
            d["tags"] = ",".join(j['topics'])
            docs.append(d)

print "loaded ",len(docs)," documents"

from  seldon.text import DocumentSimilarity,DefaultJsonCorpus
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

corpus = DefaultJsonCorpus(docs)
ds = DocumentSimilarity(model_type='gensim_lsi')
ds.fit(corpus)
print "built model"

import seldon
rw = seldon.Recommender_wrapper()
rw.save_recommender(ds,"reuters_recommender")
print "saved recommender"



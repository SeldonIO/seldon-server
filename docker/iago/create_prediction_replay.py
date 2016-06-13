import os
import sys, getopt, argparse
import logging
from random import randint,random,uniform
import json
import urllib

PREDICT_TEMPLATE = 'http://%ENDPOINT%/js/predict?json=%JSON%&consumer_key=%KEY%&jsonpCallback=j'

class ReplayCreate(object):

    def __init__(self):
        self.features = []

    def get_key(self,filename):
        with open(filename) as f:
            for line in f:
                line = line.rstrip()
                j = json.loads(line)
                self.key = j[0]["key"]

    def parse_features(self,features):
        for f in features:
            j = json.loads(f)
            self.features.append(j)

    def construct_json(self):
        j = {}
        for f in self.features:
            
            if f["type"] == "numeric":
                fval = uniform(f["min"],f["max"])
                j[f["name"]] = fval
        return json.dumps(j)

    def create_replay(self,endpoint,filename,num):
        with open(filename,"w") as f:
            for i in range (0,num):
                jStr = self.construct_json()
                jEncoded = urllib.quote_plus(jStr)
                url = PREDICT_TEMPLATE.replace("%ENDPOINT%",endpoint).replace("%KEY%",self.key).replace("%JSON%",jEncoded)+"\n"
                f.write(url)

if __name__ == '__main__':
    import logging
    logger = logging.getLogger()
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(name)s : %(message)s', level=logging.DEBUG)
    logger.setLevel(logging.INFO)

    parser = argparse.ArgumentParser(prog='create_replay')
    parser.add_argument('--endpoint', help='endpoint for seldon server', default="seldon-server")
    parser.add_argument('--key', help='file containing output of seldon-cli keys call', required=True)
    parser.add_argument('--replay', help='replay file to create', required=True)
    parser.add_argument('--num', help='number of actions and recommendation pair calls to create', required=False, type=int, default=1000)
    parser.add_argument('--feature', help='feature to add ', required=True, action='append')

    args = parser.parse_args()
    opts = vars(args)

    rc = ReplayCreate()
    rc.get_key(args.key)
    rc.parse_features(args.feature)
    rc.create_replay(args.endpoint,args.replay,args.num)

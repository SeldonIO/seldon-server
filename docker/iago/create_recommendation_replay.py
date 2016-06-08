import os
import sys, getopt, argparse
import logging
from random import randint,random
import json

RECOMMENDATION_TEMPLATE = 'http://seldon-server/js/recommendations?consumer_key=%KEY%&user=%USER%&item=%ITEM%&limit=10&jsonpCallback=j&dimensions=1'
ACTION_TEMPLATE = 'http://seldon-server/js/action/new?consumer_key=%KEY%&user=%USER%&item=%ITEM%&type=1&jsonpCallback=j'
ACTION_CLICK_TEMPLATE = 'http://seldon-server/js/action/new?consumer_key=%KEY%&user=%USER%&item=%ITEM%&type=1&rlabs=1&jsonpCallback=j'

class ReplayCreate(object):

    def __init__(self,click_percent=0.1):
        self.click_percent=click_percent

    def get_key(self,filename):
        with open(filename) as f:
            for line in f:
                line = line.rstrip()
                j = json.loads(line)
                self.key = j[0]["key"]

    def get_items(self,filename):
        self.items = {}
        i = 0
        with open(filename) as f:
            for line in f:
                line = line.rstrip()
                j = json.loads(line)
                items = j["list"]
                for item in items:
                    idx = item['id']
                    self.items[i] = idx
                    i += 1


    def create_replay(self,filename,num_actions,num_users):
        with open(filename,"w") as f:
            for i in range (0,num_actions):
                user = str(randint(1,num_users))
                item_idx = randint(0,len(self.items)-1)
                item = self.items[item_idx]
                url = RECOMMENDATION_TEMPLATE.replace("%KEY%",self.key).replace("%USER%",user).replace("%ITEM%",item)+"\n"
                f.write(url)
                p = random()
                if p < self.click_percent:
                    url = ACTION_CLICK_TEMPLATE
                else:
                    url = ACTION_TEMPLATE
                url = url.replace("%KEY%",self.key).replace("%USER%",user).replace("%ITEM%",item)+"\n"        
                f.write(url)

if __name__ == '__main__':
    import logging
    logger = logging.getLogger()
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(name)s : %(message)s', level=logging.DEBUG)
    logger.setLevel(logging.INFO)

    parser = argparse.ArgumentParser(prog='create_replay')
    parser.add_argument('--key', help='file containing output of seldon-cli keys call', required=True)
    parser.add_argument('--items', help='file containing output of seldon-cli api /items call', required=True)
    parser.add_argument('--replay', help='replay file to create', required=True)
    parser.add_argument('--num-actions', help='number of actions and recommendation pair calls to create', required=False, type=int, default=1000)
    parser.add_argument('--num-users', help='number of users to create recommendation calls for', required=False, type=int, default=1000)

    args = parser.parse_args()
    opts = vars(args)

    rc = ReplayCreate()
    rc.get_key(args.key)
    rc.get_items(args.items)
    rc.create_replay(args.replay,args.num_actions,args.num_users)

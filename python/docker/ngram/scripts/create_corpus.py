import sys, getopt, argparse
from seldon.fileutil import *
import json
import collections
from dateutil.parser import parse
import datetime
import nltk
from sklearn.externals import joblib

class NgramModel:
    
    def __init__(self,key="",secret=""):
        self.userItems = {}
        self.count = 0
        self.fu = FileUtil(aws_key=key,aws_secret=secret)
        self.epoch = datetime.datetime.utcfromtimestamp(0)

    def unix_time(self,dt):
        delta = dt - self.epoch
        return delta.total_seconds()

    def collect_sessions(self,line):
        if len(line) > 0:
            j = json.loads(line)
            userId = j['userid']
            itemId = j['itemid']
            itemId = int(itemId)
            if itemId > 0:
                if not userId in self.userItems:
                    self.userItems[userId] = []
                dt = parse(j['timestamp_utc'])
                naiveDt = dt.replace(tzinfo=None)
                epoch = self.unix_time(naiveDt)
                self.userItems[userId].append((itemId,epoch))
                self.count += 1
                if self.count % 10000 == 0:
                    print "Processed ",self.count
                    sys.stdout.flush()

    def stream(self,bucket,day):
        filePath = "s3://"+bucket+"/"+args.client+"/actions/"+str(day)+"/part"
        print filePath
        print args.bucket
        self.fu.stream_multi([filePath],self.collect_sessions)
        print "total users so far ",len(self.userItems)

    def save_sessions(self,filename):
        with open(filename,"w") as f:
            processed = 0
            for user in self.userItems:
                sessions = self.userItems[user]
                if len(sessions) > 1:
                    processed += 1
                    sessions = sorted(sessions,key=lambda x: x[1])
                    items = [item for (item,_) in sessions]
                    flist = []
                    last = None
                    for item in items:
                        if last is None:
                            flist.append(item)
                        elif not item == last:
                            flist.append(item)
                        last = item
                    if len(flist) > 1:
                        f.write(" ".join(str(x) for x in flist))
                        f.write("\n")
            f.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='create corpus')
    parser.add_argument('--client', help='client', required=True)
    parser.add_argument('--corpus', help='corpus file to create', required=True)
    parser.add_argument('--bucket', help='bucket', required=True)
    parser.add_argument('--startDay', help='day to start' , type=int, required=True)
    parser.add_argument('--numDays', help='number of days to get data for' , type=int, default=1)
    parser.add_argument('--aws_key', help='aws key - needed if input or output is on AWS and no IAM', required=True)
    parser.add_argument('--aws_secret', help='aws secret - needed if input or output on AWS  and no IAM', required=True)

    args = parser.parse_args()
    opts = vars(args)
               
    model = NgramModel(key=args.aws_key,secret=args.aws_secret)
    for day in range(args.startDay-args.numDays+1,args.startDay+1):
        print "streaming day ",day
        model.stream(args.bucket,day)
        model.save_sessions(args.corpus)

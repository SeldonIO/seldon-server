import os
import sys, getopt, argparse
import logging
import json
import time
import requests

class BadCallError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)


class LatencyTest(object):

    def __init__(self):
        self.latency_sum = 0

    def callRest(self,json,url,key):
        params = {}
        params["consumer_key"] = key
        params["json"] = json
        params["jsonpCallback"] = "unused"
        time1 = time.time()
        r = requests.get(url+"/js/predict",params=params)
        time2 = time.time()
        self.latency_sum += ((time2-time1)*1000.0)
        if r.status_code == requests.codes.ok:
            return
        else:
            raise BadCallError("bad http reponse "+str(r.status_code))


    def run(self,json_filename,url,key,num_requests):
        reqs = 0
        while True:
            with open(json_filename) as f:
                for line in f:
                    line = line.rstrip();
                    data = json.loads(line)
                    callJson = {"data":data}
                    callJsonStr = json.dumps(callJson)
                    self.callRest(callJsonStr,url,key)
                    reqs += 1
                    if reqs >= num_requests:
                        return
            
    def print_stats(self,num_requests):
        print "%d calls, avg %0.3f ms" % (num_requests,self.latency_sum/num_requests)

if __name__ == '__main__':
    import logging
    logger = logging.getLogger()
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(name)s : %(message)s', level=logging.DEBUG)
    logger.setLevel(logging.INFO)

    parser = argparse.ArgumentParser(prog='create_replay')
    parser.add_argument('--json', help='json file', required=True)
    parser.add_argument('--server-url', help='server url', required=True)
    parser.add_argument('--key', help='js key', required=True)
    parser.add_argument('--num-requests', help='number of requests to run',type=int,default=1)

    args = parser.parse_args()
    opts = vars(args)
    lt = LatencyTest()

    lt.run(args.json,args.server_url,args.key,args.num_requests)
    lt.print_stats(args.num_requests)

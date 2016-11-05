import os
import sys, getopt, argparse
import logging
import json
from google.protobuf import json_format
import seldon.rpc.seldon_pb2 as seldon_pb2
from google.protobuf import any_pb2
import requests
import grpc
import time

class LatencyTest(object):

    def __init__(self):
        self.latency_sum = 0

    def getToken(self,url,key,secret):
        params = {}
        params["consumer_key"] = key
        params["consumer_secret"] = secret
        r = requests.get(url+"/token",params=params)
        if r.status_code == requests.codes.ok:
            print r.text
            j = json.loads(r.text)
            return j["access_token"]
        else:
            print "failed call to get token"
            return None

    def callRpc(self,token,data,stub):
        dataAny = any_pb2.Any()
        dataAny.Pack(data)
        meta = seldon_pb2.ClassificationRequestMeta(puid="12345")
        request = seldon_pb2.ClassificationRequest(meta=meta,data=dataAny)
        metadata = [(b'oauth_token', token)]
        time1 = time.time()
        reply = stub.Predict(request,999,metadata=metadata)
        time2 = time.time()
        self.latency_sum += ((time2-time1)*1000.0)

    def run(self,json_filename,klass,stub,num_requests):
        reqs = 0
        while True:
            with open(json_filename) as f:
                for line in f:
                    line = line.rstrip();
                    message = json_format.Parse(line, klass())                  
                    self.callRpc(token,message,stub)
                    reqs += 1
                    if reqs % 10 == 0:
                        print reqs
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
    parser.add_argument('--proto-python-path', help='path to python generated protobuf', required=True)
    parser.add_argument('--request-class', help='request class as x.y.z', required=True)
    parser.add_argument('--json', help='json file', required=True)
    parser.add_argument('--oauth-url', help='oauth url', required=True)
    parser.add_argument('--grpc-endpoint', help='grpc endpoint as host:port', required=True)
    parser.add_argument('--key', help='oauth consumer key',required=True)
    parser.add_argument('--secret', help='oauth consumer secret',required=True)
    parser.add_argument('--num-requests', help='number of requests to run',type=int,default=1)

    args = parser.parse_args()
    opts = vars(args)
    lt = LatencyTest()

    # create request class
    sys.path.append(args.proto_python_path)
    parts = args.request_class.split(".")
    className = parts[-1]
    mod = __import__(".".join(parts[0:-1]), fromlist=[className])
    klass = getattr(mod, className)

    #get token from oauth server
    token = lt.getToken(args.oauth_url,args.key,args.secret)

    if not token is None:
        #create channel to rpc server
        channel = grpc.insecure_channel(args.grpc_endpoint)
        stub = seldon_pb2.ClassifierStub(channel)    

        lt.run(args.json,klass,stub,args.num_requests)
        lt.print_stats(args.num_requests)
    else:
        print "failed to get oauth token"

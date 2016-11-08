import os
import sys, getopt, argparse
import logging
import json
import grpc
import iris_pb2
import seldon_pb2
from google.protobuf import any_pb2
import requests

class IrisRpcClient(object):

    def __init__(self,host,http_transport,http_port,rpc_port):
        self.host = host
        self.http_transport = http_transport
        self.http_port = http_port
        self.rpc_port = rpc_port


    def getToken(self,key,secret):
        params = {}
        params["consumer_key"] = key
        params["consumer_secret"] = secret
        url = self.http_transport+"://"+self.host+":"+str(self.http_port)+"/token"
        r = requests.get(url,params=params)
        if r.status_code == requests.codes.ok:
            j = json.loads(r.text)
            return j["access_token"]
        else:
            print "failed call to get token"
            return None

    def callRpc(self,token,jStr):
        j = json.loads(jStr)
        
        channel = grpc.insecure_channel(self.host+':'+str(self.rpc_port))
        stub = seldon_pb2.ClassifierStub(channel)

        data = iris_pb2.IrisPredictRequest(f1=j["f1"],f2=j["f2"],f3=j["f3"],f4=j["f4"])
        dataAny = any_pb2.Any()
        dataAny.Pack(data)
        meta = seldon_pb2.ClassificationRequestMeta(puid="12345")
        request = seldon_pb2.ClassificationRequest(meta=meta,data=dataAny)
        metadata = [(b'oauth_token', token)]
        reply = stub.Predict(request,999,metadata=metadata)
        print reply

if __name__ == '__main__':
    import logging
    logger = logging.getLogger()
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(name)s : %(message)s', level=logging.DEBUG)
    logger.setLevel(logging.INFO)

    parser = argparse.ArgumentParser(prog='create_replay')
    parser.add_argument('--host', help='rpc server host', default="localhost")
    parser.add_argument('--http-transport', help='http or https', default="http")
    parser.add_argument('--http-port', help='http server port', type=int, default=30015)
    parser.add_argument('--rpc-port', help='rpc server port', type=int, default=30017)
    parser.add_argument('--key', help='oauth consumer key')
    parser.add_argument('--secret', help='oauth consumer secret')
    parser.add_argument('--features-json', help='JSON features to use for prediction, should contain features f1,f2,f3,f4 as floats', required=True)

    args = parser.parse_args()
    opts = vars(args)
    rpc = IrisRpcClient(host=args.host,http_transport=args.http_transport,http_port=args.http_port,rpc_port=args.rpc_port)
    token = rpc.getToken(args.key,args.secret)
    if not token is None:
        print "Got token:",token
        rpc.callRpc(token,args.features_json)
    else:
        print "failed to get token"

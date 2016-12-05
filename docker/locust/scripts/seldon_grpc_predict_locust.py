from locust.stats import RequestStats
from locust import Locust, TaskSet, task, events
import os
import sys, getopt, argparse
from random import randint,random
import json
from locust.events import EventHook
import requests
import re
import grpc
from seldon.rpc import seldon_pb2
from google.protobuf import any_pb2
import time

def parse_arguments():
    parser = argparse.ArgumentParser(prog='locust')
    parser.add_argument('--host')
    parser.add_argument('--clients')
    parser.add_argument('--hatch-rate')
    parser.add_argument('--master', action='store_true')
    args, unknown = parser.parse_known_args() 
    #args = parser.parse_args()
    opts = vars(args)
    print args
    return args.host, int(args.clients), int(args.hatch_rate)

HOST, MAX_USERS_NUMBER, USERS_PER_SECOND = parse_arguments()


slaves_connect = []
slave_report = EventHook()
ALL_SLAVES_CONNECTED = False
SLAVES_NUMBER = 1
def on_my_event(client_id,data):
    """
    Waits for all slaves to be connected and launches the swarm
    :param client_id:
    :param data:
    :return:
    """
    global ALL_SLAVES_CONNECTED
    if not ALL_SLAVES_CONNECTED:
        print "Event was fired with arguments"
        if client_id not in slaves_connect:
            slaves_connect.append(client_id)
        if len(slaves_connect) == SLAVES_NUMBER:
            print "All Slaves Connected"
            ALL_SLAVES_CONNECTED = True
            print events.slave_report._handlers
            header = {'Content-Type': 'application/x-www-form-urlencoded'}
            r = requests.post('http://127.0.0.1:8089/swarm',data={'hatch_rate':USERS_PER_SECOND,'locust_count':MAX_USERS_NUMBER},headers=header)
import resource

rsrc = resource.RLIMIT_NOFILE
soft, hard = resource.getrlimit(rsrc)
print 'RLIMIT_NOFILE soft limit starts as  :', soft

#resource.setrlimit(rsrc, (65535, hard)) #limit to one kilobyte

soft, hard = resource.getrlimit(rsrc)
print 'RLIMIT_NOFILE soft limit changed to :', soft

events.slave_report += on_my_event # Register method in slaves report event



class GrpcLocust(Locust):
    """
    This is the abstract Locust class which should be subclassed. It provides an XML-RPC client
    that can be used to make XML-RPC requests that will be tracked in Locust's statistics.
    """
    def __init__(self, *args, **kwargs):
        super(GrpcLocust, self).__init__(*args, **kwargs)
        #self.client = XmlRpcClient(self.host)



class ApiUser(GrpcLocust):
    
    min_wait=900    # Min time between requests of each user
    max_wait=1100    # Max time between requests of each user
    stop_timeout= 1000000  # Stopping time
    

    class task_set(TaskSet):

        def getEnviron(self,key,default):
            if key in os.environ:
                return os.environ[key]
            else:
                return default

        def getToken(self):
            consumer_key = self.getEnviron('SELDON_OAUTH_KEY',"oauthkey")
            consumer_secret = self.getEnviron('SELDON_OAUTH_SECRET',"oauthsecret")

            params = {}
            params["consumer_key"] = consumer_key
            params["consumer_secret"] = consumer_secret
            url = self.oauth_endpoint+"/token"
            r = requests.get(url,params=params)
            if r.status_code == requests.codes.ok:
                j = json.loads(r.text)
                print j
                return j["access_token"]
            else:
                print "failed call to get token"
                return None


        def on_start(self):
            """
            get token
            :return:
            """
            print "on start"
            self.oauth_endpoint = self.getEnviron('SELDON_OAUTH_ENDPOINT',"http://127.0.0.1:30015")
            self.token = self.getToken()
            self.grpc_endpoint = self.getEnviron('SELDON_GRPC_ENDPOINT',"127.0.0.1:30017")
            

        @task
        def get_prediction(self):
            channel = grpc.insecure_channel(self.grpc_endpoint)
            stub = seldon_pb2.SeldonStub(channel)
            fake_data = [random() for i in range(0,784)]
            data = seldon_pb2.DefaultCustomPredictRequest(values=fake_data)
            dataAny = any_pb2.Any()
            dataAny.Pack(data)
            meta = seldon_pb2.ClassificationRequestMeta(puid=str(randint(0,99999999)))
            metadata = [(b'oauth_token', self.token)]
            request = seldon_pb2.ClassificationRequest(meta=meta,data=dataAny)
            start_time = time.time()
            try:
                reply = stub.Classify(request,999,metadata=metadata)
                print reply
            except xmlrpclib.Fault as e:
                total_time = int((time.time() - start_time) * 1000)
                events.request_failure.fire(request_type="grpc", name=HOST, response_time=total_time, exception=e)
            else:
                total_time = int((time.time() - start_time) * 1000)
                events.request_success.fire(request_type="grpc", name=HOST, response_time=total_time, response_length=0)


        





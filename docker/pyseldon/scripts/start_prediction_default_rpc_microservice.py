from concurrent import futures
import time
import sys, getopt, argparse
import seldon.pipeline.util as sutl
import random
import grpc
import google.protobuf
from google.protobuf import any_pb2
import pandas as pd 
from seldon.microservice.rpc import CustomDataHandler
from seldon.microservice import Microservices

if __name__ == "__main__":
    import logging
    logger = logging.getLogger()
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(name)s : %(message)s', level=logging.DEBUG)
    logger.setLevel(logging.INFO)


    parser = argparse.ArgumentParser(prog='microservice')
    parser.add_argument('--model-name', help='name of model', required=True)
    parser.add_argument('--pipeline', help='location of prediction pipeline', required=True)
    parser.add_argument('--aws-key', help='aws key', required=False)
    parser.add_argument('--aws-secret', help='aws secret', required=False)

    args = parser.parse_args()
    opts = vars(args)

    m = Microservices(aws_key=args.aws_key,aws_secret=args.aws_secret)
    m.create_prediction_rpc_microservice(args.pipeline,args.model_name)



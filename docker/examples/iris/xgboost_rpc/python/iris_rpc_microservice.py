from concurrent import futures
import time
import sys, getopt, argparse
import seldon.pipeline.util as sutl
import random
import iris_pb2
import grpc
import google.protobuf
from google.protobuf import any_pb2
import pandas as pd 
from seldon.microservice.rpc import CustomDataHandler
from seldon.microservice import Microservices

_ONE_DAY_IN_SECONDS = 60 * 60 * 24

class BadDataError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class IrisCustomDataHandler(CustomDataHandler):

    def getData(self, request):
        anyMsg = request.data
        dc = iris_pb2.IrisPredictRequest()
        success = anyMsg.Unpack(dc)
        if success:
            df = pd.DataFrame([{"f1":dc.f1,"f2":dc.f2,"f3":dc.f3,"f4":dc.f4}])
            return df
        else:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details('Invalid data')
            raise BadDataError('Invalid data')


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
    cd = IrisCustomDataHandler()
    m.create_prediction_rpc_microservice(args.pipeline,args.model_name,cd)



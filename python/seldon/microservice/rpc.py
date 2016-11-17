from concurrent import futures
import time
import sys, getopt, argparse
import seldon.pipeline.util as sutl
import random
import seldon.rpc.seldon_pb2 as seldon_pb2
import grpc
import google.protobuf
from google.protobuf import any_pb2
import numpy as np

_ONE_DAY_IN_SECONDS = 60 * 60 * 24

class CustomDataHandler():

    def getData(request):
        return pd.DataFrame()


class BadDataError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class DefaultCustomDataHandler(CustomDataHandler):

    def getData(self, request):
        anyMsg = request.data
        dc = seldon_pb2.DefaultCustomPredictRequest()
        success = anyMsg.Unpack(dc)
        if success:
            x = np.array(dc.values)
            x = x.reshape(1, -1)
            return x
        else:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details('Invalid data')
            raise BadDataError('Invalid data')


class RpcClassifier(seldon_pb2.SeldonServicer):

    def __init__(self,pipeline,model_name,custom_data_handler=DefaultCustomDataHandler()):
        self.pipeline = pipeline
        self.model_name = model_name
        self.custom_data_handler = custom_data_handler

    def Classify(self, request, context):
        print request # custom prediction data
        df = self.custom_data_handler.getData(request)
        preds = self.pipeline.predict_proba(df)
        idMap = self.pipeline._final_estimator.get_class_id_map()
        recs_list=[]
        for index, proba in enumerate(preds[0]):
            if index in idMap:
                indexName = idMap[index]
            else:
                indexName = str(index)
            recs_list.append(seldon_pb2.ClassificationResult(prediction=float(proba),predictedClass=indexName,confidence=float(proba)))
        meta = seldon_pb2.ClassificationReplyMeta(modelName=self.model_name)
        predictions = seldon_pb2.ClassificationReply(meta=meta,predictions=recs_list)
        return predictions


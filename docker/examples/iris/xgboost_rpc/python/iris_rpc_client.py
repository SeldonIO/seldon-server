import grpc
import iris_pb2
import seldon.rpc.seldon_pb2 as seldon_pb2
from google.protobuf import any_pb2

channel = grpc.insecure_channel('localhost:50051')
stub = seldon_pb2.ClassifierStub(channel)

data = iris_pb2.IrisPredictRequest(f1=1.0,f2=0.2,f3=2.1,f4=1.2)
dataAny = any_pb2.Any()
dataAny.Pack(data)
meta = seldon_pb2.ClassificationRequestMeta(puid="12345")
request = seldon_pb2.ClassificationRequest(meta=meta,data=dataAny)
reply = stub.Predict(request)
print reply


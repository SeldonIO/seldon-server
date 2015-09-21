import sys, getopt, argparse
from seldon.fileutil import *
import json

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='setup.py')
    parser.add_argument('--client', help='client', required=True)
    parser.add_argument('--day', help='days to get features data for' , type=int)
    parser.add_argument('--inputPath', help='path to data (aws s3 or local)', required=True)
    parser.add_argument('--zkHosts', help='zookeeper')
    parser.add_argument('--awsKey', help='aws key - needed if input or output is on AWS and no IAM', default=None)
    parser.add_argument('--awsSecret', help='aws secret - needed if input or output on AWS  and no IAM', default=None)
    parser.add_argument('--svmFeatures', help='the feature containing the svm style features')

    args = parser.parse_args()
    opts = vars(args)


    conf = {}
    for k in opts:
        if opts[k]:
            if k == "features" or k == "namespaces":
                conf[k] = json.loads(opts[k])
            else:
                conf[k] = opts[k]
    print conf

    featuresModelPath = args.inputPath + "/" + args.client + "/models/" + str(args.day)
    xgboostModelPath = args.inputPath + "/" + args.client + "/xgboost/" + str(args.day)
    print "features path",featuresModelPath
    conf["featuresPath"] = featuresModelPath
    print "model path",xgboostModelPath
    conf["modelPath"] = xgboostModelPath

    #create server config
    f = open("./server_config.py","w")
    f.write("PREDICTION_ALG=\"xgboost_runtime\"\n")
    f.write("XGBOOST="+json.dumps(conf,sort_keys=True)+"\n")
    f.close()

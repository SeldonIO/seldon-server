import sys, getopt, argparse
from seldon.fileutil import *
import json

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='setup.py')
    parser.add_argument('--model_path', help='path to model folder (local or AWS s3)', required=True)
    parser.add_argument('--awsKey', help='aws key - needed if input or output is on AWS and no IAM', default=None)
    parser.add_argument('--awsSecret', help='aws secret - needed if input or output on AWS  and no IAM', default=None)

    args = parser.parse_args()
    opts = vars(args)
    
    # copy files to local directory
    fileUtil = FileUtil(key=args.awsKey,secret=args.awsSecret)
    fileUtil.copy(args.model_path+"/model","./model")
    fileUtil.copy(args.model_path+"/target_map.json","./target_map.json")

    #create server config
    f = open("./target_map.json")
    for line in f:
        line = line.rstrip()
        targetMap = json.loads(line)
        break
    conf = {}
    conf["raw_predictions"] = "./raw_predictions.txt"
    conf["classIds"] = targetMap
    f.close()
    f = open("./server_config.py","w")
    f.write("PREDICTION_ALG=\"vw\"\n")
    f.write("VW="+json.dumps(conf,sort_keys=True)+"\n")
    f.close()

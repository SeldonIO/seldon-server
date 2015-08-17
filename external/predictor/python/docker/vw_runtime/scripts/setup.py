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
    parser.add_argument('--namespaces', help='JSON providing per feature namespace mapping - default is no namespaces')
    parser.add_argument('--include', help='include these features', nargs='*')
    parser.add_argument('--exclude', help='exclude these features' , nargs='*')
    parser.add_argument('--target', help='target feature (should contain integer ids in range 1..Num Classes)')



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

    vwModelPath = args.inputPath + "/" + args.client + "/vw/"+ str(args.day) 
    featuresModelPath = args.inputPath + "/" + args.client + "/models/" + str(args.day)
    print "model path",vwModelPath
    print "features path",featuresModelPath
    conf["featuresPath"] = featuresModelPath
    # copy files to local directory
    fileUtil = FileUtil(key=args.awsKey,secret=args.awsSecret)
    fileUtil.copy(vwModelPath,"vw_model")
    #fileUtil.copy(vwModelPath+"/target_map.json","./target_map.json")

    #create server config
    f = open("./vw_model/target_map.json")
    for line in f:
        line = line.rstrip()
        targetMap = json.loads(line)
        break
    conf["raw_predictions"] = "./raw_predictions.txt"
    conf["classIds"] = targetMap
    f.close()
    f = open("./server_config.py","w")
    f.write("PREDICTION_ALG=\"vw_runtime\"\n")
    f.write("VW="+json.dumps(conf,sort_keys=True)+"\n")
    f.close()

import sys, getopt, argparse
from seldon.xgb import *
import json


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='xgboost_train')
    parser.add_argument('--client', help='client', required=True)
    parser.add_argument('--zkHosts', help='zookeeper')
    parser.add_argument('--inputPath', help='input base folder to find features data')
    parser.add_argument('--outputPath', help='output folder to store model')
    parser.add_argument('--day', help='days to get features data for' , type=int)
    parser.add_argument('--activate', help='activate model in zookeeper', action='store_true')
    parser.add_argument('--awsKey', help='aws key - needed if input or output is on AWS and no IAM')
    parser.add_argument('--awsSecret', help='aws secret - needed if input or output on AWS  and no IAM')
    parser.add_argument('--svmFeatures', help='the JSON feature containing the svm features')
    parser.add_argument('--target', help='target feature (should contain integer ids in range 1..Num Classes)')
    parser.add_argument('--target_readable', help='the feature containing the human readable version of target')


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

    xg = XGBoostSeldon(conf)
    xg.train()

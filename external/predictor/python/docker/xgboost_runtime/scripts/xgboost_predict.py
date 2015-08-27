import sys, getopt, argparse
from seldon.xgb import *
import json


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='xgboost_train')
    parser.add_argument('--client', help='client', required=True)
    parser.add_argument('--day', help='days to get features data for' , type=int)
    parser.add_argument('--inputPath', help='base input path')
    parser.add_argument('--testPath', help='input folder for test data')
    parser.add_argument('--zkHosts', help='zookeeper')
    parser.add_argument('--awsKey', help='aws key - needed if input or output is on AWS and no IAM', default=None)
    parser.add_argument('--awsSecret', help='aws secret - needed if input or output on AWS  and no IAM', default=None)
    parser.add_argument('--svmFeatures', help='the feature containing the svm style features')
    parser.add_argument('--target', help='target feature (should contain integer ids in range 1..Num Classes)')


    args = parser.parse_args()
    opts = vars(args)

    conf = {}
    for k in opts:
        if opts[k]:
            conf[k] = opts[k]
    print conf

    xg = XGBoostSeldon(conf)
    xg.load_models()
    print xg.test(args.testPath)

import sys, getopt, argparse
from seldon.vw import *
import json


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='vw')
    parser.add_argument('--client', help='client', required=True)
    parser.add_argument('--zkHosts', help='zookeeper')
    parser.add_argument('--inputPath', help='input base folder to find features data')
    parser.add_argument('--outputPath', help='output folder to store model')
    parser.add_argument('--day', help='days to get features data for' , type=int)
    parser.add_argument('--activate', help='activate model in zookeeper', action='store_true')
    parser.add_argument('--awsKey', help='aws key - needed if input or output is on AWS and no IAM')
    parser.add_argument('--awsSecret', help='aws secret - needed if input or output on AWS  and no IAM')
    parser.add_argument('--vwArgs', help='vw training args')
    parser.add_argument('--namespaces', help='JSON providing per feature namespace mapping - default is no namespaces')
    parser.add_argument('--include', help='include these features', nargs='*')
    parser.add_argument('--exclude', help='exclude these features' , nargs='*')
    parser.add_argument('--target', help='target feature (should contain integer ids in range 1..Num Classes)')
    parser.add_argument('--target_readable', help='the feature containing the human readable version of target')
    parser.add_argument('--train_filename', help='convert data to vw training format and save to file rather than directly train using wabbit_wappa')
    parser.add_argument('--dataType', help='json or csv', default="json")


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
    train_filename = opts.get("train_filename",None)
    vw = VWSeldon(**conf)
    vw.train(train_filename)

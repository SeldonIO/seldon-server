import sys, getopt, argparse
from seldon.vw import *
import json


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='vw')
    parser.add_argument('--client', help='client', required=True)
    parser.add_argument('--zkHosts', help='zookeeper')
    parser.add_argument('--inputPath', help='input base folder to find features data' , default="/seldon-models")
    parser.add_argument('--outputPath', help='output folder to store model' , default="/seldon-models")
    parser.add_argument('--startDay', help='day to start findind data and to store model in' , default=1, type=int)
    parser.add_argument('--numDays', help='number of days to get features data for' , default=1, type=int)
    parser.add_argument('--activate', help='activate model in zookeeper', action='store_true')
    parser.add_argument('--awsKey', help='aws key - needed if input or output is on AWS and no IAM')
    parser.add_argument('--awsSecret', help='aws secret - needed if input or output on AWS  and no IAM')
    parser.add_argument('--vwArgs', help='vw training args')
    parser.add_argument('--features', help='JSON providing per feature vw mapping presently label,split - default is try to create feature values as float otherwise treat as categorical')
    parser.add_argument('--namespaces', help='JSON providing per feature namespace mapping - default is no namespaces')

    args = parser.parse_args()
    opts = vars(args)

    vwArgs = {"zk_hosts" : opts.get('zkHosts'), "awsKey" : opts.get('awsKey'), "awsSecret" : opts.get('awsSecret') }
    vw = VWSeldon(**vwArgs)
    conf = {}
    for k in opts:
        if opts[k]:
            if k == "features" or k == "namespaces":
                conf[k] = json.loads(opts[k])
            else:
                conf[k] = opts[k]
    print conf
    vw.train(args.client,conf)

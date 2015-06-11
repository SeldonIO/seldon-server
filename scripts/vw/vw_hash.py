#!/usr/bin/env python
import sys, getopt, argparse
import mmh3
import math

def is_number(s):
    try:
        int(s)
        return True
    except ValueError:
        return False

def get_hash(label,namespace,feature,stride,mask):
    if namespace:
        namespace_hash = mmh3.hash(namespace,0)
    else:
        namespace_hash = 0
    if is_number(feature):
        feature_hash = int(feature) + namespace_hash
    else:
        feature_hash = mmh3.hash(feature,namespace_hash)
    feature_hash_oaa = feature_hash * stride
    return (feature_hash_oaa + label - 1) & mask

def get_constant_hash(label,stride,mask):
    feature_hash_oaa = 11650396 * stride
    return (feature_hash_oaa + label - 1) & mask
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='vw_hash')
    parser.add_argument('--oaa', help='oaa value - will assume binary if not specified', required=False, default=1, type=int)
    parser.add_argument('-b', help='bits - the hash bit sized used for training - defaults to 18', required=False, default=18, type=int)

    args = parser.parse_args()
    opts = vars(args)
    mask = (2**opts['b'])-1
    stride =  int(2**math.ceil(math.log(opts['oaa'],2)))

    for line in sys.stdin:
        line = line.rstrip()
        parts = line.split()
        label = int(parts[0])
        if label < 0:
            label = 1
        namespace = None
        initialNameSpaceSeen = False
        for token in parts:
            if token.find('|') > -1:
                namespace = token.split("|")[1]
                initialNameSpaceSeen = True
            elif initialNameSpaceSeen:
                if token.find(':') > 0:
                    (feature,value) = token.split(':')
                    print feature+":"+str(get_hash(label,namespace,feature,stride,mask))+":"+value,
                else:
                    print token+":"+str(get_hash(label,namespace,token,stride,mask)),
        print "Constant:"+str(get_constant_hash(label,stride,mask))

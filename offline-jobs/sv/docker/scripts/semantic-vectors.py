#!/usr/bin/env python
import sys, getopt, argparse
from kazoo.client import KazooClient
import json
from subprocess import call
from os import makedirs
import os
import math
from filechunkio import FileChunkIO
from shutil import copy
from boto.s3.connection import S3Connection

class SemanticVectorsError(Exception):
    pass

def loadZookeeperOptions(opts,zk):
    node = "/all_clients/"+opts['client']+"/offline/semvec"
    if zk.exists(node):
        data, stat = zk.get(node)
        jStr = data.decode("utf-8")
        print "Found zookeeper configuration:",jStr
        j = json.loads(jStr)
        for key in j:
            opts[key] = j[key]

def activateModel(args,folder,zk):
    node = "/all_clients/"+args.client+"/svtext"
    print "Activating model in zookeper at node ",node," with data ",folder
    if zk.exists(node):
        zk.set(node,folder)
    else:
        zk.create(node,folder,makepath=True)


def uploadModel(args,folder):
    if folder.startswith("/"):
        copyToLocalFolder(folder)
    elif folder.startswith("s3:/"):
        copyToS3(args,folder[4:])
    elif folder.startswith("s3n:/"):
        copyToS3(args,folder[5:])

def copyToLocalFolder(folder):
    try:
        makedirs(folder)
    except:
        print folder,"exists"
    copy("./docvectors.txt",folder+"/docvectors.txt")
    copy("./termvectors.txt",folder+"/termvectors.txt")



def copyToS3(args,folder):
    if args.awskey:
        c = S3Connection(args.awskey, args.awssecret)
    else:
        c = S3Connection()
    bucketName = folder.split('/')[1]
    path = "/".join(folder.split('/')[2:])
    b = c.get_bucket(bucketName)
    # Get file info
    source_path = './docvectors.txt'
    source_size = os.stat(source_path).st_size
    # Create a multipart upload request
    uploadPath = path + "/" + os.path.basename(source_path)
    print "uploading to bucket ",bucketName," path ",uploadPath
    mp = b.initiate_multipart_upload(uploadPath)
    # Use a chunk size of 50 MiB (feel free to change this)
    chunk_size = 10485760
    chunk_count = int(math.ceil(source_size / float(chunk_size)))
    for i in range(chunk_count):
        offset = chunk_size * i
        bytes = min(chunk_size, source_size - offset)
        with FileChunkIO(source_path, 'r', offset=offset,
                         bytes=bytes) as fp:
            print "uploading to s3 chunk ",(i+1),"/",chunk_count
            mp.upload_part_from_file(fp, part_num=i + 1)
    # Finish the upload
    print "completing transfer to s3"
    mp.complete_upload()

def createLuceneIndex(args):
    params = ["java", "-cp", "/semvec/semvec-lucene-tools.jar", "io.seldon.semvec.CreateLuceneIndexFromDb","-l","index","-jdbc",args.jdbc,"-itemType",str(args.itemType),"-raw-ids","-use-item-attrs","-attr-names",args.tagAttrs,"-recreate","-debug","-item-limit",str(args.itemLimit)]
    res = call(params)
    if res > 0:
        raise LuceneError("Failed to create lucene index")

def createSV(args):
    params = ["java","-cp","/semvec/semvec-lucene-tools.jar","pitt.search.semanticvectors.BuildIndex","-trainingcycles","1","-maxnonalphabetchars","-1","-minfrequency","0","-maxfrequency","1000000","-luceneindexpath","index","-indexfileformat","text"]
    res = call(params)
    if res > 0:
        raise SemanticVectorsError("Failed to create semantic vectors index")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='semantic-vectors')
    parser.add_argument('-c', '--client', help='client', required=True)
    parser.add_argument('-z', '--zookeeper', help='zookeeper')
    parser.add_argument('--tagAttrs', help='attribute names in Seldon DB for textual item meta data')
    parser.add_argument('--outputPath', help='output folder to store model' , default="/seldon-models")
    parser.add_argument('--startDay', help='day to store model in' , default="1")
    parser.add_argument('--activate', help='activate model', action='store_true')
    parser.add_argument('--awskey', help='aws key')
    parser.add_argument('--awssecret', help='aws secret')
    parser.add_argument('--itemLimit', help='max number of items to take from db', default="-1")
    parser.add_argument('--itemType', help='item type to restrict items to', default="1")

    args = parser.parse_args()
    opts = vars(args)

    if args.zookeeper:
        zk = KazooClient(hosts=args.zookeeper)
        zk.start()
        loadZookeeperOptions(opts,zk)
        print "tagAttrs=",args.tagAttrs
        createLuceneIndex(args)
        createSV(args)
        folder = str(args.outputPath + "/" + args.client + "/svtext/" + str(args.startDay))
        uploadModel(args,folder)
        if args.activate:
            activateModel(args,folder,zk)
        zk.stop()



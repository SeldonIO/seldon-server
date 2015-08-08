__authour__ = 'Clive Cox'
import sys
import zlib
import boto
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import glob
from shutil import copyfile
import os
import math
from filechunkio import FileChunkIO

class FileUtil:

    def __init__(self, key = None, secret = None):
        self.key = key
        self.secret = secret

    def stream_decompress(self,stream):
        dec = zlib.decompressobj(16+zlib.MAX_WBITS)  # same as gzip module
        for chunk in stream:
            rv = dec.decompress(chunk)
            if rv:
                yield rv

    def stream_text(self,k,fn):
        unfinished = ""
        for data in k:
            data = unfinished + data
            lines = data.split("\n");
            unfinished = lines.pop()
            for line in lines:
                fn(line)

    def stream_gzip(self,k,fn):
        unfinished = ""
        for data in self.stream_decompress(k):
            data = unfinished + data
            lines = data.split("\n");
            unfinished = lines.pop()
            for line in lines:
                fn(line)

    def getFolders(self,baseFolder,startDay,numDays):
        folders = []
        for day in range(startDay-numDays+1,startDay+1):
            folders.append(baseFolder+str(day)+"/*")
        return folders


    def stream_local(self,folders,fn):
        for folder in folders:
            for f in glob.glob(folder):
                k = open(f,"r")
                if f.endswith(".gz"):
                    self.stream_gzip(k,fn)
                else:
                    self.stream_text(k,fn)

    def copy_local(self,fromPath,toPath):
        print "copy ",fromPath,"to",toPath
        dir = os.path.dirname(toPath)
        if len(dir) > 0 and not os.path.exists(dir):
            os.makedirs(dir)
        copyfile(fromPath,toPath)


    def getGlob(self,startDay,numDays):
        g = "{" + str(startDay)
        for day in range(startDay-numDays+1,startDay):
            g += ","+str(day)
        g += "}"
        return g

    def stream_s3(self,bucket,prefix,fn):
        if self.key:
            self.conn = boto.connect_s3(self.key,self.secret)
        else:
            self.conn = boto.connect_s3()
        b = self.conn.get_bucket(bucket)
        for k in b.list(prefix=prefix):
            print k.name
            if k.name.endswith(".gz"):
                self.stream_gzip(k,fn)
            else:
                self.stream_text(k,fn)

    def copy_s3(self,fromPath,bucket,path):
        if self.key:
            self.conn = boto.connect_s3(self.key,self.secret)
        else:
            self.conn = boto.connect_s3()
        print fromPath, bucket, path
        b = self.conn.get_bucket(bucket)
        source_size = os.stat(fromPath).st_size
        # Create a multipart upload request
        uploadPath = path
        print "uploading to bucket ",bucket," path ",uploadPath
        mp = b.initiate_multipart_upload(uploadPath)
        chunk_size = 10485760
        chunk_count = int(math.ceil(source_size / float(chunk_size)))
        for i in range(chunk_count):
            offset = chunk_size * i
            bytes = min(chunk_size, source_size - offset)
            with FileChunkIO(fromPath, 'r', offset=offset,bytes=bytes) as fp:
                print "uploading to s3 chunk ",(i+1),"/",chunk_count
                mp.upload_part_from_file(fp, part_num=i + 1)
        # Finish the upload
        print "completing transfer to s3"
        mp.complete_upload()

    def download_s3(self,bucket,s3path,localPath):
        if self.key:
            self.conn = boto.connect_s3(self.key,self.secret)
        else:
            self.conn = boto.connect_s3()
        print bucket, s3path, localPath
        b = self.conn.get_bucket(bucket)
        key = b.get_key(s3path)
        key.get_contents_to_filename(localPath)

    def stream(self,inputPath,fn):
        if inputPath.startswith("s3n://"):
            isS3 = True
            inputPath = inputPath[6:]
        elif inputPath.startswith("s3://"):
            isS3 = True
            inputPath = inputPath[5:]
        else:
            isS3 = False
        if isS3:
            print "AWS S3 input path ",inputPath
            parts = inputPath.split('/')
            bucket = parts[0]
            prefix = inputPath[len(bucket)+1:]
            self.stream_s3(bucket,prefix,fn)
        else:
            folders = [inputPath+"*"]
            print "local input folders: ",folders
            self.stream_local(folders,fn)

    def upload(self,path,outputPath):
        if outputPath.startswith("s3n://"):
            noSchemePath = outputPath[6:]
            isS3 = True
        elif outputPath.startswith("s3://"):
            noSchemePath = outputPath[5:]
            isS3 = True
        else:
            isS3 = False
        if isS3:
            parts = noSchemePath.split('/')
            bucket = parts[0]
            opath = noSchemePath[len(bucket)+1:]
            self.copy_s3(path,bucket,opath)
        else:
            self.copy_local(path,outputPath)

    def download(self,fromPath,toPath):
        if fromPath.startswith("s3n://"):
            isS3 = True
            fromPath = fromPath[6:]
        elif fromPath.startswith("s3://"):
            isS3 = True
            fromPath = inputPath[5:]
        else:
            isS3 = False
        if isS3:
            print "AWS S3 input path ",fromPath
            parts = fromPath.split('/')
            bucket = parts[0]
            prefix = fromPath[len(bucket)+1:]
            self.download_s3(bucket,prefix,toPath)
        else:
            self.copy_local(fromPath,toPath)
        

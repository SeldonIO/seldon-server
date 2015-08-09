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
        if os.path.isfile(fromPath):
            dir = os.path.dirname(toPath)
            if len(dir) > 0 and not os.path.exists(dir):
                os.makedirs(dir)
            copyfile(fromPath,toPath)
        elif os.path.isdir(fromPath):
            if not os.path.exists(toPath):
                os.makedirs(toPath)
            for f in glob.glob(fromPath+"/*"):
                basename = os.path.basename(f)
                fnew = toPath+"/"+basename
                print "copying ",f,"to",fnew
                copyfile(f,fnew)



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


    def copy_s3_file(self,fromPath,bucket,path):
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
            folders = [inputPath+"/*"]
            print "local input folders: ",folders
            self.stream_local(folders,fn)

    def upload_s3(self,fromPath,toPath):
        if toPath.startswith("s3n://"):
            noSchemePath = toPath[6:]
        elif toPath.startswith("s3://"):
            noSchemePath = toPath[5:]
        parts = noSchemePath.split('/')
        bucket = parts[0]
        opath = noSchemePath[len(bucket)+1:]
        if os.path.isfile(fromPath):
            self.copy_s3_file(fromPath,bucket,opath)
        elif os.path.isdir(fromPath):
            for f in glob.glob(fromPath+"/*"):
                basename = os.path.basename(f)
                fnew = opath+"/"+basename
                print "copying ",f,"to",fnew
                self.copy_s3_file(f,bucket,fnew)

    def download_s3(self,fromPath,toPath):
        if fromPath.startswith("s3n://"):
            noSchemePath = fromPath[6:]
        elif fromPath.startswith("s3://"):
            noSchemePath = fromPath[5:]
        parts = noSchemePath.split('/')
        bucket = parts[0]
        s3path = noSchemePath[len(bucket)+1:]
        if self.key:
            self.conn = boto.connect_s3(self.key,self.secret)
        else:
            self.conn = boto.connect_s3()
        print bucket, s3path, toPath
        b = self.conn.get_bucket(bucket)
        for k in b.list(prefix=s3path):
            basename = os.path.basename(k.name)
            fnew = toPath+"/"+basename
            print "copying ",k.name,"to",fnew
            k.get_contents_to_filename(fnew)


    def copy(self,fromPath,toPath):
        if fromPath.startswith("s3n://") or fromPath.startswith("s3://"):
            fromS3 = True
        else:
            fromS3 = False
        if toPath.startswith("s3n://") or toPath.startswith("s3://"):
            toS3 = True
        else:
            toS3 = False
        if not fromS3 and not toS3:
            self.copy_local(fromPath,toPath)
        elif not fromS3 and toS3:
            self.upload_s3(fromPath,toPath)
        elif fromS3 and not toS3:
            if os.path.isdir(toPath):
                self.download_s3(fromPath,toPath)
            else:
                print "Local destination folder must exist :",toPath
        else:
            print "can't copy from s3 to s3"
            

        

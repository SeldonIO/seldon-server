__authour__ = 'Clive Cox'
import sys
import zlib
import boto
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import glob
from shutil import copyfile
import os

class FileUtil:

    def stream_decompress(self,stream):
        dec = zlib.decompressobj(16+zlib.MAX_WBITS)  # same as gzip module
        for chunk in stream:
            rv = dec.decompress(chunk)
            if rv:
                yield rv

    def stream_text(self,k,cl):
        unfinished = ""
        for data in k:
            data = unfinished + data
            lines = data.split("\n");
            unfinished = lines.pop()
            for line in lines:
                cl.process(line)

    def stream_gzip(self,k,cl):
        unfinished = ""
        for data in self.stream_decompress(k):
            data = unfinished + data
            lines = data.split("\n");
            unfinished = lines.pop()
            for line in lines:
                cl.process(line)

'''
Local File Stream
'''                   
class LocalFileUtil(FileUtil):         
    
    def getFolders(self,baseFolder,startDay,numDays):
        folders = []
        for day in range(startDay-numDays+1,startDay+1):
            folders.append(baseFolder+str(day)+"/*")
        return folders


    def stream(self,folders,cl):
        for folder in folders:
            for f in glob.glob(folder):
                k = open(f,"r")
                if f.endswith(".gz"):
                    self.stream_gzip(k,cl)
                else:
                    self.stream_text(k,cl)



    def copy(self,fromPath,toPath):
        dir = os.path.dirname(toPath)
        if not os.path.exists(dir):
            os.makedirs(dir)
        copyfile(fromPath,toPath)

'''
AWS S3 File Stream
'''            
class S3FileUtil(FileUtil):

    def __init__(self, key = None, secret = None):
        self.key = key
        self.secret = secret
        if key:
            self.conn = boto.connect_s3(key,secret)
        else:
            self.conn = boto.connect_s3()

    def getGlob(self,startDay,numDays):
        g = "{" + str(startDay)
        for day in range(startDay-numDays+1,startDay):
            g += ","+str(day)
        g += "}"
        return g

    def stream(self,bucket,prefix,cl):
        b = self.conn.get_bucket(bucket)
        for k in b.list(prefix=prefix):
            print k.name
            if k.name.endswith(".gz"):
                self.stream_gzip(k,cl)
            else:
                self.stream_text(k,cl)

    def copy(self,fromPath,bucket,path):
        print fromPath, bucket, path
        b = self.conn.get_bucket(bucket)
        k = b.new_key(path)
        k.set_contents_from_filename(fromPath)

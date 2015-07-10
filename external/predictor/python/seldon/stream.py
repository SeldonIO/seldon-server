__authour__ = 'Clive Cox'
import sys
import zlib
import boto
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import glob

class Stream:

    def stream_decompress(self,stream):
        dec = zlib.decompressobj(16+zlib.MAX_WBITS)  # same as gzip module
        for chunk in stream:
            rv = dec.decompress(chunk)
            if rv:
                yield rv

    def stream_text(self,k,func):
        unfinished = ""
        for data in k:
            data = unfinished + data
            lines = data.split("\n");
            unfinished = lines.pop()
            for line in lines:
                func(line)

    def stream_gzip(self,k,func):
        unfinished = ""
        for data in self.stream_decompress(k):
            data = unfinished + data
            lines = data.split("\n");
            unfinished = lines.pop()
            for line in lines:
                func(line)

'''
Local File Stream
'''                   
class FileStream(Stream):         
    def stream(self,prefix,func):
        for f in glob.glob(prefix):
            k = open(f,"r")
            if f.endswith(".gz"):
                self.stream_gzip(k,func)
            else:
                self.stream_text(k,func)

'''
AWS S3 File Stream
'''            
class S3Stream(Stream):

    def __init__(self, key = None, secret = None):
        self.key = key
        self.secret = secret
        if key:
            self.conn = boto.connect_s3(key,secret)
        else:
            self.conn = boto.connect_s3()


    def stream(self,bucket,prefix,func):
        b = self.conn.get_bucket(bucket)
        for k in b.list(prefix=prefix):
            print k.name
            if k.name.endswith(".gz"):
                self.stream_gzip(k,func)
            else:
                self.stream_text(k,func)

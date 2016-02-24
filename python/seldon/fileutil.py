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
import logging
import smart_open

logger = logging.getLogger(__name__)

class FileUtil:
    """utilities to input and output files. Locally or from AWS S3.

    Args:
        key [Optional(str)]: aws key

        secret [Optional(str)]: aws secret
    """
    def __init__(self, aws_key = None, aws_secret = None):
        self.aws_key = aws_key
        self.aws_secret = aws_secret


    #
    # Streaming
    #

    def stream_decompress(self,stream):
        """decompress a stream
        """
        dec = zlib.decompressobj(16+zlib.MAX_WBITS)  # same as gzip module
        for chunk in stream:
            rv = dec.decompress(chunk)
            if rv:
                yield rv

    def stream_gzip(self,k,fn):
        """stream from gzip file and call function for each line
        
        Args:
            k (file): input gziped file
            fn (function): function to call for each line
        """
        unfinished = ""
        for data in self.stream_decompress(k):
            data = unfinished + data
            lines = data.split("\n");
            unfinished = lines.pop()
            for line in lines:
                fn(line)

    def stream_other(self,path,fn):
        """stream from local folders call a function

        Args:
            folders (list): list of folders
            fn (function): function to call
        """
        parsed_uri = smart_open.ParseUri(path)
        if parsed_uri.scheme in ("file", ) and os.path.isdir(path): # stream each file from directory
            for f in glob.glob(path+"/*"):
                for line in smart_open.smart_open(f):
                    fn(line)
        else: #let smart_open handle streaming of file from location 
            for line in smart_open.smart_open(path):
                fn(line)

    def stream_s3(self,bucket,prefix,fn):
        """stream from an AWS S3 bucket all files under a prefix and call a function

        Args:
            bucket (str): name of S3 bucket
            prefix (str): prefix in bucket
            fn (function): function to call for each line
        """
        if self.aws_key:
            self.conn = boto.connect_s3(self.aws_key,self.aws_secret)
        else:
            self.conn = boto.connect_s3()
        b = self.conn.get_bucket(bucket)
        for k in b.list(prefix=prefix):
            if k.name.endswith(".gz"): #smart_open can't currently handle gzip on s3...
                self.stream_gzip(k,fn)
            else:
                for line in smart_open.smart_open(k):
                    fn(line)

    def stream_multi(self,inputPaths,fn):
        """ stream multilple paths calling a function on each line

        Args:
            inputPaths (list): list of input folders
            fn (function): function to call
        """
        for path in inputPaths:
            self.stream(path,fn)

    def stream(self,inputPath,fn):
        """stream from an inputpath calling function

        Args:
            inputPath (str): input path to stream from
            fn (function): function to call
        """
        if inputPath.startswith("s3n://"):
            isS3 = True
            inputPath = inputPath[6:]
        elif inputPath.startswith("s3://"):
            isS3 = True
            inputPath = inputPath[5:]
        else:
            isS3 = False
        if isS3:
            logger.info("AWS S3 input path %s",inputPath)
            parts = inputPath.split('/')
            bucket = parts[0]
            prefix = inputPath[len(bucket)+1:]
            self.stream_s3(bucket,prefix,fn)
        else:
            self.stream_other(inputPath,fn)


    #
    # Copying
    #

    def copy_local(self,fromPath,toPath):
        """copy local folders

        Args:
            fromPath (str): local from path to copy all files under
            toPath (str): local destination folder (will be created if does not exist)
        """
        logger.info("copy %s to %s",fromPath,toPath)
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
                logger.info("copying %s to %s",f,fnew)
                copyfile(f,fnew)

            
    def copy_s3_file(self,fromPath,bucket,path):
        """copy from local file to S3 

        Args:
            fromPath (str): local file
            bucket (str): S3 bucket
            path (str): S3 prefix to add to files
        """
        if self.aws_key:
            self.conn = boto.connect_s3(self.aws_key,self.aws_secret)
        else:
            self.conn = boto.connect_s3()
        b = self.conn.get_bucket(bucket)
        source_size = os.stat(fromPath).st_size
        # Create a multipart upload request
        uploadPath = path
        logger.info("uploading to bucket %s path %s",bucket,uploadPath)
        mp = b.initiate_multipart_upload(uploadPath)
        chunk_size = 10485760
        chunk_count = int(math.ceil(source_size / float(chunk_size)))
        for i in range(chunk_count):
            offset = chunk_size * i
            bytes = min(chunk_size, source_size - offset)
            with FileChunkIO(fromPath, 'r', offset=offset,bytes=bytes) as fp:
                logger.info("uploading to s3 chunk %d/%d",(i+1),chunk_count)
                mp.upload_part_from_file(fp, part_num=i + 1)
        # Finish the upload
        logger.info("completing transfer to s3")
        mp.complete_upload()

    

    def upload_s3(self,fromPath,toPath):
        """upload from local path to S3

        Args:
            fromPath (str): folder to copy from
            toPath (str): S3 URL
        """
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
                logger.info("copying %s to %s",f,fnew)
                self.copy_s3_file(f,bucket,fnew)

    def download_s3(self,fromPath,toPath):
        """download from S3 to local folder

        Args:
            fromPath (str): S3 URL
            toPath (str): local folder
        """
        if fromPath.startswith("s3n://"):
            noSchemePath = fromPath[6:]
        elif fromPath.startswith("s3://"):
            noSchemePath = fromPath[5:]
        parts = noSchemePath.split('/')
        bucket = parts[0]
        s3path = noSchemePath[len(bucket)+1:]
        if self.aws_key:
            self.conn = boto.connect_s3(self.aws_key,self.aws_secret)
        else:
            self.conn = boto.connect_s3()
        b = self.conn.get_bucket(bucket)
        for k in b.list(prefix=s3path):
            if not k.name.endswith("/"):
                basename = os.path.basename(k.name)
                fnew = toPath+"/"+basename
                logger.info("copying %s to %s",k.name,fnew)
                k.get_contents_to_filename(fnew)


    def copy(self,fromPath,toPath):
        """copy files. local->local, S3->local, local->S3 (S3->S3 not supported)
        
        Args:
            fromPath (str): local or S3 URL
            toPath (str): local or S3 URL
        """
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
                logger.error("Local destination folder must exist :%s",toPath)
        else:
            logger.warn("can't copy from s3 to s3")
            

        

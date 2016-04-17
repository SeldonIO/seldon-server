import luigi
from luigi.s3 import S3FlagTarget
from subprocess import call
import logging
from seldon.misc.item_similarity import *

#
# Item Similarity
#

class ItemSimilaritySparkJob(luigi.Task):
    """
    Spark job for running item similarity model
    """
    inputPath = luigi.Parameter(default="/seldon-data/seldon-models/")
    outputPath = luigi.Parameter(default="/seldon-data/seldon-models/")
    client = luigi.Parameter(default="test")
    startDay = luigi.IntParameter(default=1)
    days = luigi.IntParameter(default=1)
    itemType = luigi.IntParameter(-1)
    limit = luigi.IntParameter(default=100)
    minItemsPerUser = luigi.IntParameter(default=0)
    minUsersPerItem = luigi.IntParameter(default=0)    
    maxUsersPerItem = luigi.IntParameter(default=2000000)
    dimsumThreshold =luigi.FloatParameter(default=0.1)
    sample = luigi.FloatParameter()

    def output(self):
        return luigi.LocalTarget("{}/{}/item-similarity/{}".format(self.outputPath,self.client,self.startDay))

    def run(self):
        params = ["seldon-cli","model","--action","add","--client-name",self.client,"--model-name","similar-items","--inputPath",self.inputPath,"--outputPath",self.outputPath,"--startDay",str(self.startDay),"--days",str(self.days),"--sample",str(self.sample),"--itemType",str(self.itemType),"--limit",str(self.limit),"--minItemsPerUser",str(self.minItemsPerUser),"--minUsersPerItem",str(self.minUsersPerItem),"--maxUsersPerItem",str(self.maxUsersPerItem),"--dimsumthreshold",str(self.dimsumThreshold)]
        res = call(params)
        params = ["seldon-cli","model","--action","train","--client-name",self.client,"--model-name","similar-items"]
        res = call(params)
        return res


class SeldonItemSimilarity(luigi.Task):
    """
    Item similarity model. Depends on spark job. Writes results to mysql db.
    """
    startDay = luigi.IntParameter(default=1)
    client = luigi.Parameter(default="test")
    db_host = luigi.Parameter(default="mysql")
    db_user = luigi.Parameter(default="root")
    db_pass = luigi.Parameter(default="mypass")

    def requires(self):
        return ItemSimilaritySparkJob(client=self.client,startDay=self.startDay)
    
    def run(self):
        u = ItemSimilarityUploadMysql(self.client,self.db_host,self.db_user,self.db_pass)
        u.stream_and_upload(self.input().path)


#
# MF
#

class SeldonMatrixFactorization(luigi.Task):
    """
    Matrix factorization using Spark
    """
    inputPath = luigi.Parameter(default="/seldon-data/seldon-models/")
    outputPath = luigi.Parameter(default="/seldon-data/seldon-models/")
    client = luigi.Parameter(default="test")
    startDay = luigi.IntParameter(default=1)
    days = luigi.IntParameter(default=1)

    rank = luigi.IntParameter(default=30)
    mf_lambda = luigi.FloatParameter(default=0.01)
    alpha = luigi.FloatParameter(default=1)
    iterations = luigi.IntParameter(default=5)

    def output(self):
        return luigi.LocalTarget("{}/{}/matrix-factorization/{}".format(self.outputPath,self.client,self.startDay))

    def run(self):
        params = ["seldon-cli","model","--action","add","--client-name",self.client,"--model-name","matrix-factorization","--inputPath",self.inputPath,"--outputPath",self.outputPath,"--startDay",str(self.startDay),"--days",str(self.days),"--rank",str(self.rank),"--lambda",str(self.mf_lambda),"--alpha",str(self.alpha),"--iterations",str(self.iterations)]
        res = call(params)
        params = ["seldon-cli","model","--action","train","--client-name",self.client,"--model-name","matrix-factorization"]
        res = call(params)
        return res

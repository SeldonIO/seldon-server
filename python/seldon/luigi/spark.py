import luigi
from subprocess import call
import logging
from seldon.misc.item_similarity import *
from seldon.misc.most_popular import *
from luigi.contrib.spark import SparkSubmitTask

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
    sparkDriverMemory = luigi.Parameter(default="1g")
    sparkExecutorMemory = luigi.Parameter(default="1g")
    startDay = luigi.IntParameter(default=1)
    days = luigi.IntParameter(default=1)
    itemType = luigi.IntParameter(-1)
    limit = luigi.IntParameter(default=100)
    minItemsPerUser = luigi.IntParameter(default=0)
    minUsersPerItem = luigi.IntParameter(default=0)    
    maxUsersPerItem = luigi.IntParameter(default=2000000)
    dimsumThreshold =luigi.FloatParameter(default=0.1)
    sample = luigi.FloatParameter(default=1.0)

    def output(self):
        return luigi.LocalTarget("{}/{}/item-similarity/{}".format(self.outputPath,self.client,self.startDay))

    def run(self):
        params = ["seldon-cli","model","--action","add","--client-name",self.client,"--model-name","similar-items","--inputPath",self.inputPath,"--outputPath",self.outputPath,"--startDay",str(self.startDay),"--days",str(self.days),"--sample",str(self.sample),"--itemType",str(self.itemType),"--limit",str(self.limit),"--minItemsPerUser",str(self.minItemsPerUser),"--minUsersPerItem",str(self.minUsersPerItem),"--maxUsersPerItem",str(self.maxUsersPerItem),"--dimsumThreshold",str(self.dimsumThreshold)]
        res = call(params)
        params = ["seldon-cli","model","--action","train","--client-name",self.client,"--model-name","similar-items","--spark-executor-memory",self.sparkExecutorMemory,"--spark-driver-memory",self.sparkDriverMemory]
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
    sparkDriverMemory = luigi.Parameter(default="1g")
    sparkExecutorMemory = luigi.Parameter(default="1g")
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
        params = ["seldon-cli","model","--action","train","--client-name",self.client,"--model-name","matrix-factorization","--spark-executor-memory",self.sparkExecutorMemory,"--spark-driver-memory",self.sparkDriverMemory]
        res = call(params)
        return res


class SeldonMatrixFactorizationClusters(luigi.Task):
    """
    User Clustered Matrix factorization using Spark
    """
    inputPath = luigi.Parameter(default="/seldon-data/seldon-models/")
    outputPath = luigi.Parameter(default="/seldon-data/seldon-models/")
    client = luigi.Parameter(default="test")
    sparkDriverMemory = luigi.Parameter(default="1g")
    sparkExecutorMemory = luigi.Parameter(default="1g")
    startDay = luigi.IntParameter(default=1)
    days = luigi.IntParameter(default=1)

    rank = luigi.IntParameter(default=30)
    mf_lambda = luigi.FloatParameter(default=0.01)
    alpha = luigi.FloatParameter(default=1)
    iterations = luigi.IntParameter(default=5)

    def output(self):
        return luigi.LocalTarget("{}/{}/matrix-factorization-clusters/{}".format(self.outputPath,self.client,self.startDay))

    def run(self):
        params = ["seldon-cli","model","--action","add","--client-name",self.client,"--model-name","matrix-factorization-clusters","--inputPath",self.inputPath,"--outputPath",self.outputPath,"--startDay",str(self.startDay),"--days",str(self.days),"--rank",str(self.rank),"--lambda",str(self.mf_lambda),"--alpha",str(self.alpha),"--iterations",str(self.iterations)]
        res = call(params)
        params = ["seldon-cli","model","--action","train","--client-name",self.client,"--model-name","matrix-factorization-clusters","--spark-executor-memory",self.sparkExecutorMemory,"--spark-driver-memory",self.sparkDriverMemory]
        res = call(params)
        return res


class SeldonMostPopularDim(luigi.Task):
    """
    Most Popular by Dimension using Spark
    """
    inputPath = luigi.Parameter(default="/seldon-data/seldon-models/")
    outputPath = luigi.Parameter(default="/seldon-data/seldon-models/")
    client = luigi.Parameter(default="test")
    sparkDriverMemory = luigi.Parameter(default="1g")
    sparkExecutorMemory = luigi.Parameter(default="1g")
    startDay = luigi.IntParameter(default=1)
    days = luigi.IntParameter(default=1)

    k = luigi.IntParameter(default=28)
    db_host = luigi.Parameter(default="mysql")
    db_port = luigi.IntParameter(default=3306)
    db_user = luigi.Parameter(default="root")
    db_pass = luigi.Parameter(default="mypass")
    
    def output(self):
        return luigi.LocalTarget("{}/{}/mostpopulardim/{}".format(self.outputPath,self.client,self.startDay))

    def run(self):
        jdbc = "jdbc:mysql://"+self.db_host+":"+str(self.db_port)+"/"+self.client+"?characterEncoding=utf8&user="+self.db_user+"&password="+self.db_pass
        params = ["seldon-cli","model","--action","add","--client-name",self.client,"--model-name","mostpopulardim","--inputPath",self.inputPath,"--outputPath",self.outputPath,"--startDay",str(self.startDay),"--days",str(self.days),"--jdbc",jdbc,"--k",str(self.k)]
        res = call(params)
        params = ["seldon-cli","model","--action","train","--client-name",self.client,"--model-name","mostpopulardim","--spark-executor-memory",self.sparkExecutorMemory,"--spark-driver-memory",self.sparkDriverMemory]
        res = call(params)
        return res

class MostPopularSparkJob(luigi.Task):
    """
    Most Popular using Spark
    """
    inputPath = luigi.Parameter(default="/seldon-data/seldon-models/")
    outputPath = luigi.Parameter(default="/seldon-data/seldon-models/")
    client = luigi.Parameter(default="test")
    sparkDriverMemory = luigi.Parameter(default="1g")
    sparkExecutorMemory = luigi.Parameter(default="1g")
    startDay = luigi.IntParameter(default=1)
    days = luigi.IntParameter(default=1)

    def output(self):
        return luigi.LocalTarget("{}/{}/mostpopular/{}".format(self.outputPath,self.client,self.startDay))

    def run(self):
        params = ["seldon-cli","model","--action","add","--client-name",self.client,"--model-name","mostpopular","--inputPath",self.inputPath,"--outputPath",self.outputPath,"--startDay",str(self.startDay),"--days",str(self.days)]
        res = call(params)
        params = ["seldon-cli","model","--action","train","--client-name",self.client,"--model-name","mostpopular","--spark-executor-memory",self.sparkExecutorMemory,"--spark-driver-memory",self.sparkDriverMemory]
        res = call(params)
        return res


class SeldonMostPopular(luigi.Task):
    """
    Most Popular. Depends on spark job. Writes results to mysql db.
    """
    startDay = luigi.IntParameter(default=1)
    client = luigi.Parameter(default="test")
    db_host = luigi.Parameter(default="mysql")
    db_user = luigi.Parameter(default="root")
    db_pass = luigi.Parameter(default="mypass")

    def requires(self):
        return MostPopularSparkJob(client=self.client,startDay=self.startDay)
    
    def run(self):
        u = MostPopularUploadMysql(self.client,self.db_host,self.db_user,self.db_pass)
        u.stream_and_upload(self.input().path)


class SeldonSparkJob(SparkSubmitTask):
    """
    Template for running a Spark Job
    """

    app = "/home/seldon/libs/seldon-spark.jar"
    entry_class = "io.seldon.spark.mllib.SimilarItems"
    master = "spark://spark-master:7077"
    
    outputPath = luigi.Parameter(default="/seldon-data/seldon-models/")
    client = luigi.Parameter(default="test")
    startDay = luigi.IntParameter(default=17278)
                    
    def app_options(self):
        return ["--client",self.client,"--zookeeper","zookeeper-1"]

    def output(self):
        return luigi.LocalTarget("{}/{}/item-similarity/{}".format(self.outputPath,self.client,self.startDay))




import sys
from fileutil import *
from kazoo.client import KazooClient
import json
from collections import OrderedDict
import numpy as np
import xgboost
from sklearn.datasets import load_svmlight_file
import scipy.sparse


class XGBoostSeldon:
    """Seldon wrapper for training XGBoost models

       Input from local or S3. Files can be JSON or a single CSV file.
       Output model to local or S3.
    """
    def __init__(self, client=None,awsKey=None,awsSecret=None,zkHosts=None,svmFeatures={},target=None,target_readable=None,zeroBased=False,inputPath=None,day=1,outputPath=None,activate=False,**kwds):
        """Seldon XGBoost Wrapper

        Args:
            client (str): Name of client. Used to construct input and output full paths if not explicitPaths
            awsKey (Optional[str]): AWS key. Needed if using S3 paths and no IAM
            awsSecret (Optional[str]): AWS secret. Needed if using S3 paths and no IAM
            zkHosts (Optional[str]): zookeeper hosts. Used to get configuation if supplied.
            svmFeatures (Optional[dict]): dictionary of svm (numeric key) features to their values
            target (Optional[str]): name of target feature.
            target_readable (Optional[str]): name of the feature containing human readable target
            inputPath (Optional[str]): input path for features to train
            outputPath (Optional[str]): output path for vw model
            day (int): unix day number. Used to construct location of data
            activate (boolean): whether to update zookeeper with location of new model
        """
        self.client = client
        self.awsKey = awsKey
        self.awsSecret = awsSecret
        self.zk_hosts = zkHosts
        if self.zk_hosts:
            print "connecting to zookeeper at ",self.zk_hosts
            self.zk_client = KazooClient(hosts=self.zk_hosts)
            self.zk_client.start()
        else:
            self.zk_client = None
        self.conf = self.__merge_conf(self.client)
        self.svm_features = svmFeatures
        self.target= target
        self.target_readable = target_readable
        self.correct = 0
        self.predicted = 0
        self.zero_based = zeroBased
        self.inputPath = inputPath
        self.outputPath = outputPath
        self.day = day
        self.activate = activate


    def activateModel(self,client,folder):
        """activate vw model in zookeeper
        """
        node = "/all_clients/"+client+"/xgboost"
        print "Activating model in zookeper at node ",node," with data ",folder
        if self.zk_client.exists(node):
            self.zk_client.set(node,folder)
        else:
            self.zk_client.create(node,folder,makepath=True)

    def __merge_conf(self,client):
        """merge zookeeper configuration into class
        """
        thePath = "/all_clients/"+client+"/offline/vw"
        if self.zk_client and self.zk_client.exists(thePath):
            print "merging conf from zookeeper"
            data, stat = self.zk_client.get(thePath)
            zk_conf = json.loads(data.decode('utf-8'))
            for k in zk_conf:
                if not hasattr(self, k):
                    setattr(self, k, zk_conf[k])
    
    def jsonToSvm(self,j):
        """convert dictionary to svm line

        Args:
            j (dict): dictionary of SVM features. Assumes numeric keys which can be sorted and numeric values
        """
        if self.svm_features in j:
            line = str(j[self.target])
            d = j[self.svm_features]
            b = OrderedDict(sorted(d.items(), key=lambda t: float(t[0])))
            for k in b:
                line += (" "+str(k)+":"+str(b[k]))
            return line
        else:
            return None

    def process(self,line):
        """process the lines from the input and turn into SVMLight lines
        """
        j = json.loads(line)
        svmLine = self.jsonToSvm(j)
        self.numLinesProcessed += 1
        if svmLine:
            if self.target_readable:
                self.target_map[j[self.target]] = j[self.target_readable]
            if self.train_file:
                self.train_file.write(svmLine+"\n")
        
    def save_target_map(self):
        """save mapping of target ids to their descriptive meanings
        """
        v = json.dumps(self.target_map,sort_keys=True)
        f = open('./target_map.json',"w")
        f.write(v)
        f.close()

    def train_model(self):
        """train an xgboost model from generated SVM features file
        """
        train_X, train_Y = load_svmlight_file("train.svm",zero_based=self.zero_based)
        xg_train = xgboost.DMatrix(train_X,label=train_Y)
        # setup parameters for xgboost
        param = {}
        #    param['objective'] = 'multi:softmax'
        param['objective'] = 'multi:softprob'
        # scale weight of positive examples
        param['eta'] = 0.1
        param['max_depth'] = 15
        param['silent'] = 1
        param['nthread'] = 6
        param['num_class'] = len(self.target_map) + 1
        watchlist = [ (xg_train,'train') ]
        num_round = 5
        bst = xgboost.train(param, xg_train, num_round, watchlist );
        bst.save_model('model')


    def train(self):
        """train an XGBoost model using features from inputPath and putting model in outputPath
        """
        self.numLinesProcessed = 0
        self.target_map = {}
        self.train_file = open("train.svm","w")

        #stream data into vw
        inputPath = self.inputPath + "/" + self.client + "/features/" + str(self.day) + "/"
        print "inputPath->",inputPath
        fileUtil = FileUtil(key=self.awsKey,secret=self.awsSecret)
        fileUtil.stream(inputPath,self.process)
        self.train_file.close()
        
        self.train_model()

        self.save_target_map()
        # copy models to final location
        outputPath = self.outputPath + "/" + self.client + "/xgboost/" + str(self.day)
        print "outputPath->",outputPath
        fileUtil.copy("./model",outputPath+"/model")
        fileUtil.copy("./target_map.json",outputPath+"/target_map.json")

        #activate model in zookeeper
        if self.activate:
            self.activateModel(self.client,str(outputPath))


    def test(self,test_features_path):
        """test model against features from file
        """
        fileUtil = FileUtil(key=self.awsKey,secret=self.awsSecret)
        fileUtil.stream(test_features_path,self.predict_line)
        return self.get_accuracy()
        
    def load_models(self):
        """load an xgboost model
        """
        inputPath = self.inputPath + "/" + self.client + "/xgboost/" + str(self.day)
        fileUtil = FileUtil(key=self.awsKey,secret=self.awsSecret)
        fileUtil.copy(inputPath,".")
        f = open("./target_map.json")
        for line in f:
            line = line.rstrip()
            self.targetMap = json.loads(line)
            break
        self.bst = xgboost.Booster(model_file="./model")
        
    def generate_dmatrix(self,features):
        """generate dmatrix from input for xgboost model

        Args:
            features (dict): dictionary of features to score

        Returns: generated dmatricx
        """
        row = []; col = []; dat = []
        for k in features:
            colIdx = int(k)
            if not self.zero_based:
                colIdx = colIdx - 1
            row.append(0); col.append(colIdx); dat.append(float(features[k]))
        if len(row) > 0:
            csr = scipy.sparse.csr_matrix((dat, (row,col)))
        else:
            csr = scipy.sparse.csr_matrix((1, 1), dtype=np.float64)
        xg_test = xgboost.DMatrix(csr)
        return xg_test
            
    def predict_line(self,line):
        """predict line against a model

        Args:
            line (str): input feature line in SVM format
        """
        j = json.loads(line)
        yprob = self.predict(j[self.svm_features])
        correct = int(j[self.target])
        ylabel = np.argmax(yprob, axis=1)[0]
        self.predicted += 1
        if correct == ylabel:
            self.correct += 1

    def predict_json(self,j):
        """predict against a model features in j

        Args:
            j (dict): dictionary of features

        Returns: scores for each class
        """
        fscores = []
        yprob = self.predict(j[self.svm_features])
        if len(yprob) > 0:
            yprob = yprob[0]
            for i in range(1,len(yprob)):
                fscores.append((yprob[i],self.targetMap[str(i)],1.0))
        return fscores

    def predict(self,features):
        """predict againts a model using features

        Args:
            features (dict): features to predict against

        Returns: predictions
        """
        preds = self.bst.predict(self.generate_dmatrix(features))
        return preds

    def get_accuracy(self):
        """get accuracy of test
        """
        return self.correct / float(self.predicted)

import sys
from fileutil import *
from kazoo.client import KazooClient
import json
from wabbit_wappa import *
from subprocess import call
import seldon.pipeline.pipelines as pl
import numpy as np
import random
from socket import *
import threading, Queue, subprocess
import time
import psutil
import pandas as pd
from sklearn.utils import check_X_y
from sklearn.utils import check_array
from sklearn.base import BaseEstimator

class VWClassifier(pl.Estimator,pl.Feature_transform,BaseEstimator):
    """Wrapper for Vowpall Wabbit classifier

       Args:

       target (Str): Target column

       target_readable (Str): More descriptive version of target variable

       included [Optional(list(str))]: columns to include

       excluded [Optional(list(str))]: columns to exclude

       num_iterations (int): number of iterations over data to run vw

       raw_predictions (str): file to push raw predictions from vw to

       model_file (str): model filename

       pid_file (str): file to store pid of vw server so we can terminate it

       vw_args (optional vw args):arguments passed to vw
    """
    def __init__(self, target=None, target_readable=None,included=None,excluded=None,num_iterations=1, raw_predictions_file="/tmp/raw_predictions",model_file="/tmp/model",pid_file='/tmp/vw_pid_file',**vw_args):
        super(VWClassifier, self).__init__(target,target_readable,included,excluded)
        self.clf = None
        self.num_iterations = num_iterations
        self.vw_args = vw_args
        self.model_file="/tmp/model"
        self.param_suffix="_params"
        self.model_suffix="_model"
        self.raw_predictions_file=raw_predictions_file
        self.raw_predictions_thread_running = False
        self.tailq = Queue.Queue(maxsize=1000)         
        self.vw = None
        self.vw_mode = None
        self.pid_file = pid_file

    def get_models(self):
        """get model data for this transform.
        """
        return super(VWClassifier, self).get_models_estimator() + [self.num_iterations,self.vw_args,self.raw_predictions_file]
    
    def set_models(self,models):
        """set the included features
        """
        models = super(VWClassifier, self).set_models_estimator(models)
        self.num_iterations = models[0]
        self.vw_args = models[1]
        self.raw_predictions_file = models[2]

    def wait_model_saved(self,fname):
        """Hack to wait for vw model to finish saving. It creates a file <model>.writing during this process
        """
        print "waiting for ",fname
        time.sleep(1)
        while os.path.isfile(fname):
            print "sleeping until model is saved"
            time.sleep(1)

    def save_vw_model(self,fname):
        self.vw.save_model(fname)
        self.wait_model_saved(fname+".writing")

    def save_model(self,folder_prefix):
        """Save vw model from running vw instance
        """
        super(VWClassifier, self).save_model(folder_prefix+self.param_suffix)
        self.model_file=folder_prefix+self.model_suffix
        self.save_vw_model(self.model_file)

    def load_model(self,folder_prefix):
        """set model file for vw
        """
        super(VWClassifier, self).load_model(folder_prefix+self.param_suffix)
        self.model_file=folder_prefix+self.model_suffix

    @staticmethod
    def is_number(s):
        try:
            float(s)
            return True
        except ValueError:
            return False


    def get_feature(self,name,val):
        """Create a vw feature from name and value
        """
        if isinstance(val, basestring):
            if len(val) > 0:
                if self.is_number(val):
                    return (name,float(val))
                else:
                    if len(name) > 0:
                        return (name+"_"+val)
                    else:
                        return (val)
        else:
            if not np.isnan(val):
                return (name,float(val))

    def convert_row(self,row,tag=None):
        """Convert a dataframe row into a vw line
        """
        ns = {}
        ns["def"] = []
        for col in row.index.values:
            if not col == self.target:
                val = row[col]
                feature = None
                if isinstance(val,basestring):
                    feature = self.get_feature(col,val)
                    if not feature is None:
                        ns["def"].append(feature)
                elif isinstance(val,dict):
                    for key in val:
                        feature = self.get_feature(str(key),val[key])
                        if not feature is None:
                            if not col in ns:
                                ns[col] = []
                            ns[col].append(feature)
                elif isinstance(val,list):
                    for v in val:
                        feature = self.get_feature("",v)
                        if not feature is None:
                            if not col in ns:
                                ns[col] = []
                            ns[col].append(feature)
        if self.target in row:
            target = row[self.target]
        else:
            target = None
        namespaces = []
        for k in ns:
            if not k == 'def':
                namespaces.append(Namespace(name=k,features=ns[k]))
        #tag = "tag"+str(random.randrange(0,9999999))
        return self.vw.make_line(response=target,features=ns['def'],namespaces=namespaces)
    
    @staticmethod
    def sigmoid(x):
        return 1 / (1 + math.exp(-x))

    @staticmethod        
    def normalize( predictions ):
        s = sum( predictions )
        normalized = []
        for p in predictions:
            normalized.append( p / s )
        return normalized  

    def start_raw_predictions(self):
        """Start a thread to tail the raw predictions file
        """
        if not self.raw_predictions_thread_running:
            thread = threading.Thread(target=self.tail_forever, args=(self.raw_predictions_file,))
            thread.setDaemon(True)
            thread.start()
            self.raw_predictions_thread_running = True

    def close(self):
        """Shutdown the vw process
        """
        if not self.vw is None:
            f=open(self.pid_file)
            for line in f:
                print "terminating pid ",line
                p = psutil.Process(int(line))
                p.terminate()
            self.vw.close()
            self.vw = None


    def tail_forever(self,fn):
        """Tail the raw predictions file so we can get class probabilities when doing predictions
        """
        p = subprocess.Popen(["tail", "-f", fn], stdout=subprocess.PIPE)
        while 1:
            line = p.stdout.readline()
            self.tailq.put(line)
            if not line:
                break

    def get_full_scores(self):
        """Get the predictions from the vw raw predictions and normalise them
        """
        rawLine = self.tailq.get()
        parts = rawLine.split(' ')
        tagScores = parts[len(parts)-1].rstrip() 
        scores = []
        for score in parts:
            (classId,score) = score.split(':')
            scores.append(self.sigmoid(float(score)))
        nscores = self.normalize(scores)
        fscores = []
        c = 1
        for nscore in nscores:
            fscores.append(nscore)
            c = c + 1
        return np.array(fscores)

    def _exclude_include_features(self,df):
        if not self.included is None:
            print "including features ",self.included
            df = df[list(set(self.included+[self.target]).intersection(df.columns))]
        if not self.excluded is None:
            print "excluding features",self.excluded
            df = df.drop(set(self.excluded).intersection(df.columns), axis=1)
        return df


    def fit(self,X,y=None):
        """Convert data to vw lines and then train for required iterations
           
           Caveats : 
               1. A seldon specific fork of wabbit_wappa is needed to allow vw to run in server mode without save_resume. Save_resume seems to cause issues with the scores returned. Maybe connected to https://github.com/JohnLangford/vowpal_wabbit/issues/262
        """
        if isinstance(X,pd.DataFrame):
            df = X
            if not self.target_readable is None:
                self.create_class_id_map(df,self.target,self.target_readable,zero_based=False)
            df_base = self._exclude_include_features(df)
            df_base = df_base.fillna(0)
        else:
            check_X_y(X,y)
            df_X = pd.DataFrame(X)
            df_y = pd.DataFrame(y,columns=list('y'))
            self.target='y'
            df_base = pd.concat([df_X,df_y],axis=1)
            print df_base.head()

        num_classes = len(df_base[self.target].unique())
        print "num classes ",num_classes
        if self.vw is None or self.vw_mode == "test":
            if not self.vw is None:
                self.close()
            print "Creating vw in train mode"
            self.vw =  VW(server_mode=True,pid_file=self.pid_file,port=29742,num_children=1,oaa=num_classes,**self.vw_args)
            print self.vw.command
            self.vw_mode = 'train'
        df_vw = df_base.apply(self.convert_row,axis=1)
        for i in range(0,self.num_iterations):
            for (index,val) in df_vw.iteritems():
                self.vw.send_line(val,parse_result=False)
        self.save_vw_model(self.model_file)        


    def save_vw_lines(self,df,filename):
        if self.vw is None:
            self.vw =  VW(i=self.model_file)
        df_base = self._exclude_include_features(df)
        df_base = df_base.fillna(0)
        self.start_raw_predictions()
        df_vw = df_base.apply(self.convert_row,axis=1)
        df_vw.to_csv(filename,index=False,header=False)

    def predict_proba(self,X):
        """Create predictions. Start a vw process. Convert data to vw format and send. 
           Caveats : 
               1. A seldon specific fork of wabbit_wappa is needed to allow vw to run in server mode without save_resume. Save_resume seems to cause issues with the scores returned. Maybe connected to https://github.com/JohnLangford/vowpal_wabbit/issues/262
        """
        if self.vw is None or self.vw_mode == 'train':
            print "Creating vw in test mode"
            if not self.vw is None:
                self.close()
            self.vw =  VW(server_mode=True,pid_file=self.pid_file,port=29743,num_children=1,i=self.model_file,raw_predictions=self.raw_predictions_file,t=True)
            print self.vw.command
            self.vw_mode = 'test'

        if isinstance(X,pd.DataFrame):
            df = X
            df_base = self._exclude_include_features(df)
            df_base = df_base.fillna(0)
        else:
            check_array(X)
            df_base = pd.DataFrame(X)
            
        df_vw = df_base.apply(self.convert_row,axis=1)
        predictions = None
        for (index,val) in df_vw.iteritems():
            prediction = self.vw.send_line(val,parse_result=True)
            self.start_raw_predictions()
            scores = self.get_full_scores()
            if predictions is None:
                predictions = np.array([scores])
            else:
                predictions = np.vstack([predictions,scores])
        return predictions

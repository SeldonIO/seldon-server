import sys
from fileutil import *
import json
from wabbit_wappa import *
from subprocess import call
import numpy as np
import random
from socket import *
import threading, Queue, subprocess
import time
import psutil
import pandas as pd
from seldon.pipeline.pandas_pipelines import BasePandasEstimator 
from sklearn.utils import check_X_y
from sklearn.utils import check_array
from sklearn.base import BaseEstimator,ClassifierMixin
import logging

logger = logging.getLogger(__name__)

class VWClassifier(BasePandasEstimator,BaseEstimator,ClassifierMixin):
    """
    Wrapper for Vowpall Wabbit classifier with pandas support

    Parameters
    ----------
           
    target : str
       Target column
    target_readable : str
       More descriptive version of target variable
    included : list str, optional
       columns to include
    excluded : list str, optional
       columns to exclude
    id_map : dict (int,str), optional
       map of class ids to high level names
    num_iterations : int
       number of iterations over data to run vw
    raw_predictions_file : str
       file to push raw predictions from vw to
    model_file : str
       model filename
    pid_file : str
       file to store pid of vw server so we can terminate it
    vw_args : optional dict 
       extra args to pass to vw
    """
    def __init__(self, target=None, target_readable=None,included=None,excluded=None,id_map={},num_iterations=1, raw_predictions_file="/tmp/raw_predictions",model_file="/tmp/model",pid_file='/tmp/vw_pid_file',**vw_args):
        super(VWClassifier, self).__init__(target,target_readable,included,excluded,id_map)
        self.clf = None
        self.num_iterations = num_iterations
        self.model_file="/tmp/model"
        self.param_suffix="_params"
        self.model_suffix="_model"
        self.raw_predictions_file=raw_predictions_file
        self.raw_predictions_thread_running = False
        self.tailq = Queue.Queue(maxsize=1000)         
        self.vw = None
        self.vw_mode = None
        self.pid_file = pid_file
        self.vw_args = vw_args
        self.model = None
        self.model_saved = False

    def __getstate__(self):
        """
        Remove things that should not be pickled
        """
        result = self.__dict__.copy()
        del result['model_saved']
        del result['vw']
        del result['tailq']
        del result['raw_predictions_thread_running']
        return result

    def __setstate__(self, dict):
        """
        Add thread based variables when creating
        """
        self.__dict__ = dict
        self.model_saved = False
        self.vw = None
        self.tailq = Queue.Queue(maxsize=1000)      
        self.raw_predictions_thread_running=False
        if not self.model is None:
            self._start_vw_if_needed("test")
            
    def _wait_model_saved(self,fname):
        """
        Hack to wait for vw model to finish saving. It creates a file <model>.writing during this process
        """
        logger.info("waiting for %s",fname)
        time.sleep(1)
        while os.path.isfile(fname):
            logger.info("sleeping until model is saved")
            time.sleep(1)

    def _save_model(self,fname):
        """
        Save vw model from running vw instance
        """
        self.vw.save_model(fname)
        self._wait_model_saved(fname+".writing")
        with open(fname, mode='rb') as file: # b is important -> binary
            self.model = file.read()

    def _write_model(self):
        """
        Write the vw model to file
        """
        with open(self.model_file, mode='wb') as modelfile: # b is important -> binary
            modelfile.write(self.model)
            self.model_saved = True


    @staticmethod
    def _is_number(s):
        try:
            float(s)
            return True
        except ValueError:
            return False


    def _get_feature(self,name,val):
        """
        Create a vw feature from name and value
        """
        if isinstance(val, basestring):
            if len(val) > 0:
                if self._is_number(val):
                    return (name,float(val))
                else:
                    if len(name) > 0:
                        return (name+"_"+val)
                    else:
                        return (val)
        else:
            if not np.isnan(val):
                return (name,float(val))

    def _convert_row(self,row,tag=None):
        """Convert a dataframe row into a vw line
        """
        ns = {}
        ns["def"] = []
        for col in row.index.values:
            if not col == self.target:
                val = row[col]
                feature = None
                if isinstance(val,basestring):
                    feature = self._get_feature(col,val)
                    if not feature is None:
                        ns["def"].append(feature)
                elif isinstance(val,dict):
                    for key in val:
                        feature = self._get_feature(key,val[key])
                        if not feature is None:
                            if not col in ns:
                                ns[col] = []
                            ns[col].append(feature)
                elif isinstance(val,list):
                    for v in val:
                        feature = self._get_feature("",v)
                        if not feature is None:
                            if not col in ns:
                                ns[col] = []
                            ns[col].append(feature)
        if self.target in row:
            target = row[self.target]
            target = int(target)
            if self.zero_based:
                target += 1
        else:
            target = None
        namespaces = []
        for k in ns:
            if not k == 'def':
                namespaces.append(Namespace(name=k,features=ns[k]))
        return self.vw.make_line(response=target,features=ns['def'],namespaces=namespaces)
    
    @staticmethod
    def _sigmoid(x):
        return 1 / (1 + math.exp(-x))

    @staticmethod        
    def _normalize( predictions ):
        s = sum( predictions )
        normalized = []
        for p in predictions:
            normalized.append( p / s )
        return normalized  

    def _start_raw_predictions(self):
        """Start a thread to tail the raw predictions file
        """
        if not self.raw_predictions_thread_running:
            thread = threading.Thread(target=self._tail_forever, args=(self.raw_predictions_file,))
            thread.setDaemon(True)
            thread.start()
            self.raw_predictions_thread_running = True

    def close(self):
        """Shutdown the vw process
        """
        if not self.vw is None:
            f=open(self.pid_file)
            for line in f:
                logger.info("terminating pid %s",line)
                p = psutil.Process(int(line))
                p.terminate()
            self.vw.close()
            self.vw = None


    def _tail_forever(self,fn):
        """Tail the raw predictions file so we can get class probabilities when doing predictions
        """
        p = subprocess.Popen(["tail", "-f", fn], stdout=subprocess.PIPE)
        while 1:
            line = p.stdout.readline()
            self.tailq.put(line)
            if not line:
                break

    def _get_full_scores(self):
        """Get the predictions from the vw raw predictions and normalise them
        """
        rawLine = self.tailq.get()
        parts = rawLine.split(' ')
        tagScores = parts[len(parts)-1].rstrip() 
        scores = []
        for score in parts:
            (classId,score) = score.split(':')
            scores.append(self._sigmoid(float(score)))
        nscores = self._normalize(scores)
        fscores = []
        c = 1
        for nscore in nscores:
            fscores.append(nscore)
            c = c + 1
        return np.array(fscores)

    def _exclude_include_features(self,df):
        if not self.included is None:
            df = df[list(set(self.included+[self.target]).intersection(df.columns))]
        if not self.excluded is None:
            df = df.drop(set(self.excluded).intersection(df.columns), axis=1)
        return df


    def fit(self,X,y=None):
        """Convert data to vw lines and then train for required iterations
           
        Parameters
        ----------

        X : pandas dataframe or array-like
           training samples
        y : array like, required for array-like X and not used presently for pandas dataframe
           class labels

        Returns
        -------
        self: object

        Caveats : 
        1. A seldon specific fork of wabbit_wappa is needed to allow vw to run in server mode without save_resume. Save_resume seems to cause issues with the scores returned. Maybe connected to https://github.com/JohnLangford/vowpal_wabbit/issues/262
        """
        if isinstance(X,pd.DataFrame):
            df = X
            df_base = self._exclude_include_features(df)
            df_base = df_base.fillna(0)
        else:
            check_X_y(X,y)
            df = pd.DataFrame(X)
            df_y = pd.DataFrame(y,columns=list('y'))
            self.target='y'
            df_base = pd.concat([df,df_y],axis=1)

        min_target = df_base[self.target].astype(float).min()
        if min_target == 0:
            self.zero_based = True
        else:
            self.zero_based = False
        if not self.target_readable is None:
            self.create_class_id_map(df,self.target,self.target_readable,zero_based=self.zero_based)

        self.num_classes = len(df_base[self.target].unique())
        self._start_vw_if_needed("train")
        df_vw = df_base.apply(self._convert_row,axis=1)
        for i in range(0,self.num_iterations):
            for (index,val) in df_vw.iteritems():
                self.vw.send_line(val,parse_result=False)
        self._save_model(self.model_file)        
        return self

    def _start_vw_if_needed(self,mode):
        if self.vw is None or self.vw_mode != mode:
            logger.info("Creating vw in mode %s",mode)
            if not self.vw is None:
                self.close()
            if mode == "test":
                if not self.model_saved:
                    self._write_model()
                self.vw =  VW(server_mode=True,pid_file=self.pid_file,port=29743,num_children=1,i=self.model_file,raw_predictions=self.raw_predictions_file,t=True)
            else:
                self.vw =  VW(server_mode=True,pid_file=self.pid_file,port=29742,num_children=1,oaa=self.num_classes,**self.vw_args)
            logger.info(self.vw.command)
            self.vw_mode = mode
            logger.info("Created vw in mode %s",mode)


    def predict_proba(self,X):
        """Create predictions. Start a vw process. Convert data to vw format and send. 
        Returns class probability estimates for the given test data.

        X : pandas dataframe or array-like
            Test samples 
        
        Returns
        -------
        proba : array-like, shape = (n_samples, n_outputs)
            Class probability estimates.
  
        Caveats : 
        1. A seldon specific fork of wabbit_wappa is needed to allow vw to run in server mode without save_resume. Save_resume seems to cause issues with the scores returned. Maybe connected to https://github.com/JohnLangford/vowpal_wabb#it/issues/262
        """
        self._start_vw_if_needed("test")
        if isinstance(X,pd.DataFrame):
            df = X
            df_base = self._exclude_include_features(df)
            df_base = df_base.fillna(0)
        else:
            check_array(X)
            df_base = pd.DataFrame(X)
        df_vw = df_base.apply(self._convert_row,axis=1)
        predictions = None
        for (index,val) in df_vw.iteritems():
            prediction = self.vw.send_line(val,parse_result=True)
            self._start_raw_predictions()
            scores = self._get_full_scores()
            if predictions is None:
                predictions = np.array([scores])
            else:
                predictions = np.vstack([predictions,scores])
        return predictions

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

class VwClassifier(pl.Estimator,pl.Feature_transform):
    def __init__(self, target=None, target_readable=None,included=None,excluded=None,num_iterations=1, raw_predictions_file="/tmp/raw_predictions",**vw_args):
        super(VwClassifier, self).__init__()
        self.clf = None
        self.target = target
        self.target_readable = target_readable
        self.set_class_id_map({})
        self.included = included
        self.excluded = excluded
        self.num_iterations = num_iterations
        self.vw_args = vw_args
        self.model_file="/tmp/model"
        self.param_suffix="_params"
        self.model_suffix="_model"
        self.raw_predictions_file=raw_predictions_file
        self.raw_predictions_thread_running = False
        self.tailq = Queue.Queue(maxsize=1000)         
        self.vw = None

    def get_models(self):
        """get model data for this transform.
        """
        return [self.target,self.included,self.excluded,self.num_iterations,self.vw_args,self.raw_predictions_file,self.get_class_id_map()]
    
    def set_models(self,models):
        """set the included features
        """
        self.target = models[0]
        self.included = models[1]
        self.excluded = models[2]
        self.num_iterations = models[3]
        self.vw_args = models[4]
        self.raw_predictions_file = models[5]
        self.set_class_id_map(models[6])

    def wait_model_saved(self,fname):
        """Hackto wait for vw model to finish saving. It created a file <model>.writing during this process
        """
        print "waiting for ",fname
        time.sleep(1)
        while os.path.isfile(fname):
            print "sleeping until model is saved"
            time.sleep(1)

    def save_model(self,folder_prefix):
        super(VwClassifier, self).save_model(folder_prefix+self.param_suffix)
        fname = folder_prefix+self.model_suffix
        self.vw.save_model(fname)
        self.wait_model_saved(fname+".writing")

    def load_model(self,folder_prefix):
        super(VwClassifier, self).load_model(folder_prefix+self.param_suffix)
        self.model_file=folder_prefix+self.model_suffix

    @staticmethod
    def is_number(s):
        try:
            float(s)
            return True
        except ValueError:
            return False


    def get_feature(self,name,val):
        if isinstance(val, basestring):
            if len(val) > 0:
                if self.is_number(val):
                    return (name,float(val))
                else:
                    return (name+"_"+val)
        else:
            if not np.isnan(val):
                return (name,float(val))

    def convert_row(self,row,tag=None):
        ns = []
        for col in row.index.values:
            if not col == self.target:
                val = row[col]
                feature = None
                if isinstance(col,basestring):
                    feature = self.get_feature(col,val)
                elif isinstance(val,dict):
                    for key in val:
                        feature = self.get_feature(col+"_"+key,val)
                elif isinstance(val,list):
                    for v in val:
                        feature = self.get_feature(col,v)
                if not feature is None:
                    ns.append(feature)
        return self.vw.make_line(response=row[self.target],features=ns,tag=tag)
    
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

    def close(self):
        self.p.terminate()
        if not self.vw is None:
            self.vw.close()
            self.vw = None

    def tail_forever(self,fn):
        print "tailing ",fn
        self.p = subprocess.Popen(["tail", "-f", fn], stdout=subprocess.PIPE)
        while 1:
            line = self.p.stdout.readline()
            print "adding line",line
            self.tailq.put(line)
            if not line:
                break

    def get_full_scores(self):
        rawLine = self.tailq.get()
        print "looking at ",rawLine
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
        return fscores

    def _exclude_include_features(self,df):
        if not self.included is None:
            print "including features ",self.included
            df = df(self.included)
        elif not self.excluded is None:
            print "excluding features",self.excluded
            df = df.drop(set(self.excluded).intersection(df.columns), axis=1)
        return df



    def fit(self,df):
        if not self.target_readable is None:
            self.create_class_id_map(df,self.target,self.target_readable)
        df_base = self._exclude_include_features(df)
        df_base = df_base.fillna(0)

        num_classes = len(df_base[self.target].unique())
        print "num classes ",num_classes
        command = "vw --save_resume --predictions /dev/stdout"
        self.vw =  VW(oaa=num_classes,raw_predictions=self.raw_predictions_file,**self.vw_args)
        df_vw = df_base.apply(self.convert_row,axis=1)
        for i in range(0,self.num_iterations):
            for (index,val) in df_vw.iteritems():
                print "training->",val
                self.vw.send_example(val)

    def start_raw_predictions(self):
        if not self.raw_predictions_thread_running:
            threading.Thread(target=self.tail_forever, args=(self.raw_predictions_file,)).start()
            self.raw_predictions_thread_running = True

    def predict_proba(self,df):
        if self.vw is None:
            print "starting new vw"
            self.vw =  VW(i=self.model_file,raw_predictions=self.raw_predictions_file)
        self.start_raw_predictions()
        df_vw = df.apply(self.convert_row,axis=1)
        print df_vw
        predictions = []
        for (index,val) in df_vw.iteritems():
            print "sending ",val
            prediction = self.vw.get_prediction(val)
            print "prediction->",prediction
            scores = self.get_full_scores()
            print "Scores",scores
            predictions.append(scores)
        return predictions

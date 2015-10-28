import sys
from fileutil import *
from kazoo.client import KazooClient
import json
from collections import OrderedDict
import numpy as np
import xgboost as xgb
from sklearn.datasets import load_svmlight_file
import scipy.sparse
import math
import pandas as pd
from sklearn.feature_extraction import DictVectorizer
import seldon.pipeline.pipelines as pl


class XGBoostClassifier(pl.Estimator,pl.Feature_transform):
    def __init__(self, target=None, target_readable=None,included=None,excluded=None,num_rounds=10, **params):
        super(XGBoostClassifier, self).__init__()
        self.clf = None
        self.target = target
        self.target_readable = target_readable
        self.set_class_id_map({})
        self.included = included
        self.excluded = excluded
        self.vectorizer = None
        self.num_boost_round = num_rounds
        self.params = params
        self.params_suffix = "_params"
        self.model_suffix = "_model"

    def get_models(self):
        """get model data for this transform.
        """
        return [self.target,self.included,self.excluded,self.vectorizer,self.num_boost_round,self.params,self.get_class_id_map()]
    
    def set_models(self,models):
        """set the included features
        """
        self.target = models[0]
        self.included = models[1]
        self.excluded = models[2]
        self.vectorizer = models[3]
        self.num_boost_round = models[4]
        self.params = models[5]
        self.set_class_id_map(models[6])


    def save_model(self,folder_prefix):
        super(XGBoostClassifier, self).save_model(folder_prefix+self.params_suffix)
        self.clf.save_model(folder_prefix+self.model_suffix)

    def load_model(self,folder_prefix):
        super(XGBoostClassifier, self).load_model(folder_prefix+self.params_suffix)
        self.clf = xgb.Booster({'nthread':-1}) 
        self.clf.load_model(folder_prefix+self.model_suffix) 

    def get_params(self, deep=True):
        return self.params

    def set_params(self, **params):
        if 'num_boost_round' in params:
            self.num_boost_round = params.pop('num_boost_round')
        if 'objective' in params:
            del params['objective']
        self.params.update(params)
        return self

    def _exclude_include_features(self,df):
        if not self.included is None:
            print "including features ",self.included
            df = df(self.included)
        elif not self.excluded is None:
            print "excluding features",self.excluded
            df = df.drop(set(self.excluded).intersection(df.columns), axis=1)
        return df

    def fit(self,df):
        df_y = df[self.target]
        if not self.target_readable is None:
            self.create_class_id_map(df,self.target,self.target_readable)
        df_base = df.drop([self.target], axis=1)
        df_base = self._exclude_include_features(df_base)
        df_base = df_base.fillna(0)

        (df_X,self.vectorizer) = self.convert_dataframe(df_base,self.vectorizer)
        dtrain = xgb.DMatrix(df_X, label=df_y)
        watchlist = [ (dtrain,'train') ]
        self.clf = xgb.train(params=self.params, dtrain=dtrain, num_boost_round=self.num_boost_round,evals=watchlist)

    def predict(self, X):
        num2label = dict((i, label)for label, i in self.label2num.items())
        Y = self.predict_proba(X)
        y = np.argmax(Y, axis=1)
        return np.array([num2label[i] for i in y])

    def predict_proba(self, df):
        if self.target in df:
            df_y = df[self.target]
            df_base = df.drop([self.target], axis=1)
        else:
            df_y = None
            df_base = df
        df_base = self._exclude_include_features(df_base)
        df_base = df_base.fillna(0)

        (df_X,_) = self.convert_dataframe(df_base,self.vectorizer)
        dtest = xgb.DMatrix(df_X)
        res =  self.clf.predict(dtest)
        return res

    def score(self, X, y):
        Y = self.predict_proba(X)
        return 1 / logloss(y, Y)

    
    
def logloss(y_true, Y_pred):
    label2num = dict((name, i) for i, name in enumerate(sorted(set(y_true))))
    return -1 * sum(math.log(y[label2num[label]]) if y[label2num[label]] > 0 else -np.inf for y, label in zip(Y_pred, y_true)) / len(Y_pred)




import sys
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
        super(XGBoostClassifier, self).__init__(target,target_readable,included,excluded)
        self.clf = None
        self.num_boost_round = num_rounds
        self.params = params
        self.params_suffix = "_params"
        self.model_suffix = "_model"

    def get_models(self):
        """get model data for this transform.
        """
        return super(XGBoostClassifier, self).get_models_estimator() + [self.num_boost_round,self.params]
    
    def set_models(self,models):
        """set the included features
        """
        models = super(XGBoostClassifier, self).set_models_estimator(models)
        self.num_boost_round = models[0]
        self.params = models[1]

    def save_model(self,folder_prefix):
        super(XGBoostClassifier, self).save_model(folder_prefix+self.params_suffix)
        self.clf.save_model(folder_prefix+self.model_suffix)

    def load_model(self,folder_prefix):
        super(XGBoostClassifier, self).load_model(folder_prefix+self.params_suffix)
        self.clf = xgb.Booster({'nthread':-1}) 
        self.clf.load_model(folder_prefix+self.model_suffix) 


    def fit(self,df):
        (X,y,self.vectorizer) = self.convert_numpy(df)
        dtrain = xgb.DMatrix(X, label=y)
        watchlist = [ (dtrain,'train') ]
        self.clf = xgb.train(params=self.params, dtrain=dtrain, num_boost_round=self.num_boost_round,evals=watchlist)

    def predict(self, X):
        num2label = dict((i, label)for label, i in self.label2num.items())
        Y = self.predict_proba(X)
        y = np.argmax(Y, axis=1)
        return np.array([num2label[i] for i in y])

    def predict_proba(self, df):
        (X,_,_) = self.convert_numpy(df)
        dtest = xgb.DMatrix(X)
        res =  self.clf.predict(dtest)
        return res

    def score(self, X, y):
        Y = self.predict_proba(X)
        return 1 / logloss(y, Y)

    
    
def logloss(y_true, Y_pred):
    label2num = dict((name, i) for i, name in enumerate(sorted(set(y_true))))
    return -1 * sum(math.log(y[label2num[label]]) if y[label2num[label]] > 0 else -np.inf for y, label in zip(Y_pred, y_true)) / len(Y_pred)




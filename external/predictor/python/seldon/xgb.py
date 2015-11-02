import sys
import numpy as np
import xgboost as xgb
from sklearn.datasets import load_svmlight_file
import scipy.sparse
import math
import pandas as pd
from sklearn.feature_extraction import DictVectorizer
import seldon.pipeline.pipelines as pl
from collections import OrderedDict
import io

class XGBoostClassifier(pl.Estimator,pl.Feature_transform):
    def __init__(self, target=None, target_readable=None,included=None,excluded=None,dict_feature=None,num_rounds=10, **params):
        super(XGBoostClassifier, self).__init__(target,target_readable,included,excluded)
        self.clf = None
        self.num_boost_round = num_rounds
        self.params = params
        self.params_suffix = "_params"
        self.model_suffix = "_model"
        self.dict_feature = dict_feature

    def get_models(self):
        """get model data for this transform.
        """
        return super(XGBoostClassifier, self).get_models_estimator() + [self.dict_feature,self.num_boost_round,self.params]
    
    def set_models(self,models):
        """set the included features
        """
        models = super(XGBoostClassifier, self).set_models_estimator(models)
        self.dict_feature = models[0]
        self.num_boost_round = models[1]
        self.params = models[2]

    def save_model(self,folder_prefix):
        super(XGBoostClassifier, self).save_model(folder_prefix+self.params_suffix)
        self.clf.save_model(folder_prefix+self.model_suffix)

    def load_model(self,folder_prefix):
        super(XGBoostClassifier, self).load_model(folder_prefix+self.params_suffix)
        self.clf = xgb.Booster({'nthread':-1}) 
        self.clf.load_model(folder_prefix+self.model_suffix) 


    def to_svmlight(self,row):
        if self.target in row:
            line = str(row[self.target])
        else:
            line = "1"
        d = row[self.dict_feature]
        b = OrderedDict(sorted(d.items(), key=lambda t: float(t[0])))
        for k in b:
            line += (" "+str(k)+":"+str(b[k]))
        return line
        
    def load_from_dict(self,df):
        print "loading from dictionary feature"
        df_svm = df.apply(self.to_svmlight,axis=1)
        output = io.BytesIO()
        df_svm.to_csv(output,index=False,header=False)
        output.seek(0)
        (X,y) = load_svmlight_file(output,zero_based=False)
        output.close()
        return (X,y)


    def fit(self,df):
        if not self.dict_feature is None:
            if not self.target_readable is None:
                self.create_class_id_map(df,self.target,self.target_readable)
            (X,y) = self.load_from_dict(df)
            num_class = len(np.unique(y))
        else:
            (X,y,self.vectorizer) = self.convert_numpy(df)
            num_class = len(y.unique())
        print "num class",num_class
        self.params['num_class'] = num_class
        dtrain = xgb.DMatrix(X, label=y)
        watchlist = [ (dtrain,'train') ]
        self.clf = xgb.train(params=self.params, dtrain=dtrain, num_boost_round=self.num_boost_round,evals=watchlist)

    def predict(self, X):
        Y = self.predict_proba(X)
        y = np.argmax(Y, axis=1)
        return y

    def predict_proba(self, df):
        if not self.dict_feature is None:
            (X,_) = self.load_from_dict(df)
        else:
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




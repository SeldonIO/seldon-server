import sys
import numpy as np
import xgboost as xgb
from sklearn.datasets import load_svmlight_file
import scipy.sparse
import math
import pandas as pd
from sklearn.feature_extraction import DictVectorizer
from seldon.pipeline.pandas_pipelines import PandasEstimator 
from collections import OrderedDict
import io
from sklearn.utils import check_X_y
from sklearn.utils import check_array
from sklearn.base import BaseEstimator,ClassifierMixin

class XGBoostClassifier(PandasEstimator,BaseEstimator,ClassifierMixin):
    """Wrapper for XGBoost classifier

       Args:

       target (Str): Target column

       target_readable (Str): More descriptive version of target variable

       included [Optional(list(str))]: columns to include

       excluded [Optional(list(str))]: columns to exclude

       num_iterations (int): number of iterations over data to run vw

       raw_predictions (str): file to push raw predictions from vw to

       params (optional xgboost args):arguments passed to xgboost

    """
    def __init__(self, target=None, target_readable=None,included=None,excluded=None,clf=None,
                 id_map={},vectorizer=None,dict_feature=None, 
                 max_depth=3, learning_rate=0.1, n_estimators=100,
                 silent=True, objective="reg:linear",
                 nthread=-1, gamma=0, min_child_weight=1, max_delta_step=0,
                 subsample=1, colsample_bytree=1, colsample_bylevel=1,
                 reg_alpha=0, reg_lambda=1, scale_pos_weight=1,
                 base_score=0.5, seed=0, missing=None):
        super(XGBoostClassifier, self).__init__(target,target_readable,included,excluded,id_map)
        self.target = target
        self.target_readable = target_readable
        self.id_map=id_map
        self.included = included
        self.excluded = excluded
        if not self.target_readable is None:
            if self.excluded is None:
                self.excluded = [self.target_readable]
            else:
                self.excluded.append(self.target_readable)
        self.vectorizer = vectorizer
        self.clf = clf
        self.params = { "max_depth":max_depth,"learning_rate":learning_rate,"n_estimators":n_estimators,
                       "silent":silent, "objective":objective,
                       "nthread":nthread, "gamma":gamma, "min_child_weight":min_child_weight, "max_delta_step":max_delta_step,
                       "subsample":subsample, "colsample_bytree":colsample_bytree, "colsample_bylevel":colsample_bylevel,
                       "reg_alpha":reg_alpha, "reg_lambda":reg_lambda, "scale_pos_weight":scale_pos_weight,
                       "base_score":base_score, "seed":seed, "missing":missing }
        self.dict_feature = dict_feature
        

    def to_svmlight(self,row):
        """Convert a dataframe row containing a dict of id:val to svmlight line
        """
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
        """Load data from dataframe with dict of id:val into numpy matrix
        """
        print "loading from dictionary feature"
        df_svm = df.apply(self.to_svmlight,axis=1)
        output = io.BytesIO()
        df_svm.to_csv(output,index=False,header=False)
        output.seek(0)
        (X,y) = load_svmlight_file(output,zero_based=False)
        output.close()
        return (X,y)


    def fit(self,X,y=None):
        """Fit a model from various sources: 
           1. dataframe with dict of numeric id:floats converted via svmlight format lines
           2. dataframe with numeric and categorical features
        """
        if isinstance(X,pd.DataFrame):
            df = X
            if not self.dict_feature is None:
                if not self.target_readable is None:
                    self.create_class_id_map(df,self.target,self.target_readable)
                (X,y) = self.load_from_dict(df)
                num_class = len(np.unique(y))
            else:
                (X,y,self.vectorizer) = self.convert_numpy(df)
                num_class = len(y.unique())
        else:
            check_X_y(X,y)
            num_class = len(np.unique(y))

        self.clf = xgb.XGBClassifier(**self.params)
        self.clf.fit(X,y,verbose=True)
        print self.clf.get_params(deep=True)

    def predict_proba(self, X):
        """Predict from data in following formats:
           1. dataframe with dict of numeric id:floats converted via svmlight format lines
           2. dataframe with numeric and categorical features
        """
        if isinstance(X,pd.DataFrame):
            df = X
            if not self.dict_feature is None:
                (X,_) = self.load_from_dict(df)
            else:
                (X,_,_) = self.convert_numpy(df)
        else:
            check_array(X)

        return self.clf.predict_proba(X)


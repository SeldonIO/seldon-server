import sys
import numpy as np
import xgboost as xgb
from sklearn.datasets import load_svmlight_file
import scipy.sparse
import math
import pandas as pd
from sklearn.feature_extraction import DictVectorizer
from seldon.pipeline.pandas_pipelines import BasePandasEstimator 
from collections import OrderedDict
import io
from sklearn.utils import check_X_y
from sklearn.utils import check_array
from sklearn.base import BaseEstimator,ClassifierMixin
import logging

logger = logging.getLogger(__name__)

class XGBoostClassifier(BasePandasEstimator,BaseEstimator,ClassifierMixin):
    """
    Wrapper for XGBoost classifier with pandas support
    XGBoost specific arguments follow https://github.com/dmlc/xgboost/blob/master/python-package/xgboost/sklearn.py

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
    raw_predictions : str
       file to push raw predictions from vw to
    max_depth : int
        Maximum tree depth for base learners.
    learning_rate : float
        Boosting learning rate (xgb's "eta")
    n_estimators : int
        Number of boosted trees to fit.
    silent : boolean
        Whether to print messages while running boosting.
    objective : string
        Specify the learning task and the corresponding learning objective.
    nthread : int
        Number of parallel threads used to run xgboost.
    gamma : float
        Minimum loss reduction required to make a further partition on a leaf node of the tree.
    min_child_weight : int
        Minimum sum of instance weight(hessian) needed in a child.
    max_delta_step : int
        Maximum delta step we allow each tree's weight estimation to be.
    subsample : float
        Subsample ratio of the training instance.
    colsample_bytree : float
        Subsample ratio of columns when constructing each tree.
    colsample_bylevel : float
        Subsample ratio of columns for each split, in each level.
    reg_alpha : float (xgb's alpha)
        L2 regularization term on weights
    reg_lambda : float (xgb's lambda)
        L1 regularization term on weights
    scale_pos_weight : float
        Balancing of positive and negative weights.
    base_score:
        The initial prediction score of all instances, global bias.
    seed : int
        Random number seed.
    missing : float, optional
        Value in the data which needs to be present as a missing value. If
        None, defaults to np.nan.
    """
    def __init__(self, target=None, target_readable=None,included=None,excluded=None,clf=None,
                 id_map={},vectorizer=None,svmlight_feature=None, 
                 max_depth=3, learning_rate=0.1, n_estimators=100,
                 silent=True, objective="reg:linear",
                 nthread=-1, gamma=0, min_child_weight=1, max_delta_step=0,
                 subsample=1, colsample_bytree=1, colsample_bylevel=1,
                 reg_alpha=0, reg_lambda=1, scale_pos_weight=1,
                 base_score=0.5, seed=0, missing=None):
        super(XGBoostClassifier, self).__init__(target,target_readable,included,excluded,id_map)
        self.vectorizer = vectorizer
        self.clf = clf
        self.max_depth=max_depth 
        self.learning_rate=learning_rate
        self.n_estimators=n_estimators
        self.silent=silent
        self.objective=objective
        self.nthread=nthread
        self.gamma=gamma 
        self.min_child_weight=min_child_weight
        self.max_delta_step=max_delta_step
        self.subsample=subsample 
        self.colsample_bytree=colsample_bytree
        self.colsample_bylevel=colsample_bylevel
        self.reg_alpha=reg_alpha
        self.reg_lambda=reg_lambda
        self.scale_pos_weight=scale_pos_weight
        self.base_score=base_score
        self.seed=seed
        self.missing=missing
        #self.params = { "max_depth":max_depth,"learning_rate":learning_rate,"n_estimators":n_estimators,
        #               "silent":silent, "objective":objective,
        #               "nthread":nthread, "gamma":gamma, "min_child_weight":min_child_weight, "max_delta_step":max_delta_step,
        #               "subsample":subsample, "colsample_bytree":colsample_bytree, "colsample_bylevel":colsample_bylevel,
        #               "reg_alpha":reg_alpha, "reg_lambda":reg_lambda, "scale_pos_weight":scale_pos_weight,
        #               "base_score":base_score, "seed":seed, "missing":missing }
        self.svmlight_feature = svmlight_feature
        

    def _to_svmlight(self,row):
        """Convert a dataframe row containing a dict of id:val to svmlight line
        """
        if self.target in row:
            line = str(row[self.target])
        else:
            line = "1"
        d = row[self.svmlight_feature]
        for (k,v) in d:
            line += (" "+str(k)+":"+str(v))
        return line
        
    def _load_from_svmlight(self,df):
        """Load data from dataframe with dict of id:val into numpy matrix
        """
        logger.info("loading from dictionary feature")
        df_svm = df.apply(self._to_svmlight,axis=1)
        output = io.BytesIO()
        df_svm.to_csv(output,index=False,header=False)
        output.seek(0)
        (X,y) = load_svmlight_file(output,zero_based=False)
        output.close()
        return (X,y)


    def fit(self,X,y=None):
        """Fit a model: 

        Parameters
        ----------

        X : pandas dataframe or array-like
           training samples. If pandas dataframe can handle dict of feature in one column or cnvert a set of columns
        y : array like, required for array-like X and not used presently for pandas dataframe
           class labels

        Returns
        -------
        self: object


        """
        if isinstance(X,pd.DataFrame):
            df = X
            if not self.svmlight_feature is None:
                if not self.target_readable is None:
                    self.create_class_id_map(df,self.target,self.target_readable)
                (X,y) = self._load_from_svmlight(df)
                num_class = len(np.unique(y))
            else:
                (X,y,self.vectorizer) = self.convert_numpy(df)
                num_class = len(y.unique())
        else:
            check_X_y(X,y)
            num_class = len(np.unique(y))

        self.clf = xgb.XGBClassifier(max_depth=self.max_depth, learning_rate=self.learning_rate, 
                                     n_estimators=self.n_estimators,
                                     silent=self.silent, objective=self.objective,
                                     nthread=self.nthread, gamma=self.gamma, 
                                     min_child_weight=self.min_child_weight, 
                                     max_delta_step=self.max_delta_step,
                                     subsample=self.subsample, colsample_bytree=self.colsample_bytree, 
                                     colsample_bylevel=self.colsample_bylevel,
                                     reg_alpha=self.reg_alpha, reg_lambda=self.reg_lambda, 
                                     scale_pos_weight=self.scale_pos_weight,
                                     base_score=self.base_score, seed=self.seed, missing=self.missing)
        logger.info(self.clf.get_params(deep=True))
        self.clf.fit(X,y,verbose=True)
        return self

    def predict_proba(self, X):
        """
        Returns class probability estimates for the given test data.

        X : pandas dataframe or array-like
            Test samples 
        
        Returns
        -------
        proba : array-like, shape = (n_samples, n_outputs)
            Class probability estimates.
  
        """
        if isinstance(X,pd.DataFrame):
            df = X
            if not self.svmlight_feature is None:
                (X,_) = self._load_from_svmlight(df)
            else:
                (X,_,_) = self.convert_numpy(df)
        else:
            check_array(X)

        return self.clf.predict_proba(X)




import pandas as pd
import numpy as np
from bayes_opt import BayesianOptimization
from sklearn.base import BaseEstimator
import seldon.pipeline.cross_validation as cf
import logging

logger = logging.getLogger(__name__)

class BayesOptimizer(BaseEstimator):

    """
    sklearn wrapper for BayesianOptimization module

    Parameters
    ----------
    
    clf : object
       sklearn compatible estimator
    param_ranges : dict
       dict of parameters to optimize with ranges in form 'name':(min,max)
    param_int : list
       list of parameters that need conversion to int before calling estimator
    cv_folds : int
       number of cross validation folds to run

    """
    def __init__(self,clf=None,param_ranges={},param_int=[],cv_folds=5):
        self.clf = clf
        self.param_ranges=param_ranges
        self.param_int = param_int
        self.best_score = 0.0
        self.cv_folds=5
        self.X = None
        self.y = None

    def __getstate__(self):
        """
        Remove things that should not be pickled
        """
        result = self.__dict__.copy()
        del result['X']
        del result['y']
        return result


    def get_best_score(self):
        return self.best_score

    def score(self,**params):
        for v in self.param_int:
            params[v] = int(params[v])
        self.clf.set_params(**params)
        cv = cf.SeldonKFold(self.clf,self.cv_folds)
        cv.fit(self.X,self.y)
        return cv.get_score()

    def fit(self,X,y=None):
        """Fit a model: 

        Parameters
        ----------

        X : pandas dataframe or array-like
           training samples. If pandas dataframe can handle dict of feature in one column or convert a set of columns
        y : array like, required for array-like X and not used presently for pandas dataframe
           class labels

        Returns
        -------
        self: object
        """
        self.X = X
        self.y = y
        bopt = BayesianOptimization(self.score,self.param_ranges)
        bopt.maximize()
        logger.info(bopt.res)
        self.best_score = bopt.res['max']['max_val']
        params = bopt.res['max']['max_params']
        for v in self.param_int:
            params[v] = int(params[v])
        self.clf.set_params(**params)
        self.clf.fit(X,y)
        return self
        
    def transform(self,X):
        """
        Do nothing and pass input back
        """
        return X

    def predict_proba(self, X):
        return self.clf.predict_proba(X)

    def get_class_id_map(self):
        return self.clf.get_class_id_map()

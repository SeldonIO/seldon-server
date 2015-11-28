from sklearn.feature_extraction import DictVectorizer
from seldon.pipeline.pandas_pipelines import BasePandasEstimator 
from collections import OrderedDict
import io
from sklearn.utils import check_X_y
from sklearn.utils import check_array
from sklearn.base import BaseEstimator,ClassifierMixin
import pandas as pd

class SKLearnClassifier(BasePandasEstimator,BaseEstimator,ClassifierMixin):

    """
    Wrapper for XGBoost classifier with pandas support
    XGBoost specific arguments follow https://github.com/dmlc/xgboost/blob/master/python-package/xgboost/sklearn.py

    clf : sklearn estimator
       sklearn estimator to run
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
    sk_args : str, optional
       extra args for sklearn classifier
    """
    def __init__(self, clf=None,target=None, target_readable=None,included=None,excluded=None,id_map={},vectorizer=None,**sk_args):
        super(SKLearnClassifier, self).__init__(target,target_readable,included,excluded,id_map)
        self.vectorizer = vectorizer
        self.clf = clf
        self.sk_args = sk_args

    def fit(self,X,y=None):
        """
        Fit an sklearn classifier to data

        Parameters
        ----------

        X : pandas dataframe or array-like
           training samples
        y : array like, required for array-like X and not used presently for pandas dataframe
           class labels

        Returns
        -------
        self: object

        """
        if isinstance(X,pd.DataFrame):
            df = X
            (X,y,self.vectorizer) = self.convert_numpy(df)
        else:
            check_X_y(X,y)

        self.clf.fit(X,y)
        return self

    def predict_proba(self,X):
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
            (X,_,_) = self.convert_numpy(df)
        else:
            check_array(X)

        return self.clf.predict_proba(X)


    def predict(self,X):
        """
        Returns class predictions

        X : pandas dataframe or array-like
            Test samples 
        
        Returns
        -------
        proba : array-like, shape = (n_samples, n_outputs)
            Class predictions
  
        """
        if isinstance(X,pd.DataFrame):
            df = X
            (X,_,_) = self.convert_numpy(df)
        else:
            check_array(X)

        return self.clf.predict(X)

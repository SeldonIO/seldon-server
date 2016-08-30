from collections import defaultdict
import pandas as pd
import numpy as np
from sklearn.base import BaseEstimator,TransformerMixin
from seldon.util import DeprecationHelper

class SklearnTransform(BaseEstimator,TransformerMixin):
    """
    Allow sklearn transformers to be run on Pandas dataframes.

    Parameters
    ----------

    input_features : list str
       input columns to use
    output_features : list str, optional
       names of output columns
    transformer : scikit learn Transformer
       transformer to run on data
    """
    def __init__(self,input_features=None,output_features=None,output_features_prefix=None,transformer=None):
        self.input_features=input_features
        self.output_features=output_features
        self.transformer=transformer
        self.output_features_prefix=output_features_prefix


    def fit(self,df):
        """
        Parameters
        ----------

        df : pandas dataframe 

        Returns
        -------
        self: object
        """
        self.transformer.fit(df[self.input_features].values)
        return self
        
    def transform(self,df):
        """
        transform the input columns and merge result into input dataframe using column names if provided

        Parameters
        ----------

        df : pandas dataframe 

        Returns
        -------
        
        Transformed pandas dataframe
        """
        Y = self.transformer.transform(df[self.input_features].values)
        df_Y = pd.DataFrame(Y)
        if not self.output_features_prefix is None:
            cols = [self.output_features_prefix+"_"+str(c) for c in df_Y.columns]
            df_Y.columns = cols
        elif not self.output_features is None and len(df_Y.columns) == len(self.output_features):
            df_Y.columns = self.output_features
        df_2 = pd.concat([df,df_Y],axis=1)
        return df_2

sklearn_transform = DeprecationHelper(SklearnTransform)